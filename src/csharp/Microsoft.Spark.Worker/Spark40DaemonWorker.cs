// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#if NET8_0_OR_GREATER
using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Worker.Utils;

namespace Microsoft.Spark.Worker
{
    /// <summary>
    /// Spark 4 requires real Worker PIDs. Launch managed children without forking the CLR,
    /// adapting SimpleWorker's auth-then-PID handshake to the daemon's PID-then-auth protocol.
    /// </summary>
    internal sealed class Spark40DaemonWorker
    {
        internal const string ChildModule = "dotnet.daemon.worker";

        private readonly object _gate = new object();
        private readonly CancellationTokenSource _shutdown = new CancellationTokenSource();
        private readonly Dictionary<int, CancellationTokenSource> _workers =
            new Dictionary<int, CancellationTokenSource>();
        private readonly HashSet<Task> _connections = new HashSet<Task>();
        private readonly BlockingCollection<byte[]> _diagnostics =
            new BlockingCollection<byte[]>(16);

        internal void Run() => RunAsync().GetAwaiter().GetResult();

        internal static void WatchParent()
        {
            // The parent owns the only writer. EOF also handles SIGKILL and stuck user code.
            new Thread(() =>
            {
                try
                {
                    Stream input = Console.OpenStandardInput();
                    while (input.ReadByte() != -1)
                    {
                    }
                }
                finally
                {
                    Environment.Exit(0);
                }
            }) { IsBackground = true }.Start();
        }

        private async Task RunAsync()
        {
            string secret = SettingUtils.GetWorkerFactorySecret() ??
                throw new InvalidOperationException("Missing Worker factory secret.");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            new Thread(WriteDiagnostics) { IsBackground = true }.Start();
            using PosixSignalRegistration signal = OperatingSystem.IsWindows() ? null :
                PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
                {
                    context.Cancel = true;
                    _shutdown.Cancel();
                });

            try
            {
                listener.Start();
                Stream output = Console.OpenStandardOutput();
                SerDe.Write(output, ((IPEndPoint)listener.LocalEndpoint).Port);
                output.Flush();
                new Thread(ReadStopRequests) { IsBackground = true }.Start();

                while (!_shutdown.IsCancellationRequested)
                {
                    TcpClient client = await listener.AcceptTcpClientAsync(_shutdown.Token);
                    Task connection;
                    lock (_gate)
                    {
                        connection = Task.Run(() => HandleConnectionAsync(client, secret));
                        _connections.Add(connection);
                    }

                    _ = connection.ContinueWith(completed =>
                    {
                        lock (_gate)
                        {
                            _connections.Remove(completed);
                        }
                    }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
                }
            }
            catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
            {
            }
            finally
            {
                listener.Stop();
                _shutdown.Cancel();
                Task[] connections;
                lock (_gate)
                {
                    connections = _connections.ToArray();
                }

                try
                {
                    await Task.WhenAll(connections);
                }
                finally
                {
                    _diagnostics.CompleteAdding();
                }
            }
        }

        private void ReadStopRequests()
        {
            try
            {
                Stream input = Console.OpenStandardInput();
                byte[] bytes = new byte[sizeof(int)];
                while (!_shutdown.IsCancellationRequested)
                {
                    input.ReadExactly(bytes);
                    int pid = BinaryPrimitives.ReadInt32BigEndian(bytes);
                    if (pid < 0)
                    {
                        break;
                    }

                    lock (_gate)
                    {
                        if (_workers.TryGetValue(pid, out CancellationTokenSource worker))
                        {
                            worker.Cancel();
                        }
                    }
                }
            }
            catch (IOException)
            {
                // Includes EOF and a truncated PID. Neither is a valid stop request.
            }
            finally
            {
                _shutdown.Cancel();
            }
        }

        private async Task HandleConnectionAsync(TcpClient client, string secret)
        {
            using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);
            using var startup = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token);
            startup.CancelAfter(TimeSpan.FromSeconds(10));
            var listener = new TcpListener(IPAddress.Loopback, 0);
            Process child = null;
            TcpClient worker = null;
            Task logs = Task.CompletedTask;
            int pid = 0;
            try
            {
                startup.Token.ThrowIfCancellationRequested();
                listener.Start();
                string childSecret = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
                child = StartChild(((IPEndPoint)listener.LocalEndpoint).Port, childSecret);
                pid = child.Id;
                lock (_gate)
                {
                    _workers.Add(pid, lifetime);
                }

                logs = Task.WhenAll(
                    ReadDiagnosticsAsync(child.StandardOutput.BaseStream, lifetime.Token),
                    ReadDiagnosticsAsync(child.StandardError.BaseStream, lifetime.Token));
                worker = await listener.AcceptTcpClientAsync(startup.Token);
                NetworkStream workerStream = worker.GetStream();
                await AuthenticateAsync(workerStream, childSecret, startup.Token);
                if (await ReadInt32Async(workerStream, startup.Token) != pid)
                {
                    throw new IOException("Child Worker PID does not match its process.");
                }

                listener.Stop();
                NetworkStream clientStream = client.GetStream();
                await WriteInt32Async(clientStream, pid, startup.Token);
                await AuthenticateAsync(clientStream, secret, startup.Token);
                startup.CancelAfter(Timeout.InfiniteTimeSpan);
                await RelayAsync(clientStream, workerStream, child, lifetime.Token);
            }
            catch (OperationCanceledException)
            {
                if (!lifetime.IsCancellationRequested)
                {
                    Log($"Worker [{pid}] startup/authentication timed out.");
                }
            }
            catch (Exception exception)
            {
                Log($"Worker [{pid}] connection failed: {exception.Message}");
            }
            finally
            {
                listener.Stop();
                lifetime.Cancel();
                client.Dispose();
                worker?.Dispose();
                if (child != null)
                {
                    try
                    {
                        if (!child.HasExited)
                        {
                            try
                            {
                                child.Kill();
                            }
                            catch (InvalidOperationException) when (child.HasExited)
                            {
                                // Normal completion raced cancellation.
                            }
                        }

                        await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
                    }
                    catch (Exception exception)
                    {
                        Log(
                            $"Worker [{pid}] exit could not be confirmed: {exception.Message}");
                        _shutdown.Cancel();
                    }

                    await ObserveAsync(logs);
                    lock (_gate)
                    {
                        _workers.Remove(pid);
                    }

                    child.Dispose();
                }
            }
        }

        private void Log(string message)
        {
            // Keep every queued block below 4 KiB, including multibyte diagnostics.
            _diagnostics.TryAdd(Encoding.UTF8.GetBytes(
                message.Substring(0, Math.Min(message.Length, 1000)) + Environment.NewLine));
        }

        private async Task ReadDiagnosticsAsync(Stream stream, CancellationToken cancellation)
        {
            byte[] buffer = new byte[4096];
            int count;
            while ((count = await stream.ReadAsync(buffer, cancellation)) != 0)
            {
                _diagnostics.TryAdd(buffer.AsSpan(0, count).ToArray());
            }
        }

        private void WriteDiagnostics()
        {
            try
            {
                Stream error = Console.OpenStandardError();
                foreach (byte[] bytes in _diagnostics.GetConsumingEnumerable())
                {
                    error.Write(bytes);
                }
            }
            catch (IOException)
            {
                // Diagnostics are best-effort if Spark stops consuming stderr.
            }
            catch (ObjectDisposedException)
            {
                // A closed stderr must not terminate the daemon's cleanup path.
            }
        }

        private static Process StartChild(int port, string secret)
        {
            string executable = Environment.ProcessPath ??
                throw new InvalidOperationException("Cannot determine the Worker executable.");
            var start = new ProcessStartInfo(executable)
            {
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = Environment.CurrentDirectory
            };
            if (string.Equals(Path.GetFileNameWithoutExtension(executable), "dotnet",
                StringComparison.OrdinalIgnoreCase))
            {
                start.ArgumentList.Add(typeof(Program).Assembly.Location);
            }

            start.ArgumentList.Add("-m");
            start.ArgumentList.Add(ChildModule);
            start.Environment["PYTHON_WORKER_FACTORY_PORT"] = port.ToString();
            start.Environment["PYTHON_WORKER_FACTORY_SECRET"] = secret;
            return Process.Start(start) ?? throw new IOException("Unable to start Worker child.");
        }

        private static async Task AuthenticateAsync(
            NetworkStream stream, string secret, CancellationToken cancellation)
        {
            byte[] expected = Encoding.UTF8.GetBytes(secret);
            int length = await ReadInt32Async(stream, cancellation);
            bool authenticated = false;
            if (length == expected.Length)
            {
                byte[] received = new byte[length];
                await stream.ReadExactlyAsync(received, cancellation);
                authenticated = CryptographicOperations.FixedTimeEquals(received, expected);
            }

            byte[] response = Encoding.UTF8.GetBytes(authenticated ? "ok" : "err");
            await WriteInt32Async(stream, response.Length, cancellation);
            await stream.WriteAsync(response, cancellation);
            if (!authenticated)
            {
                throw new IOException("Worker authentication failed.");
            }
        }

        private static async Task<int> ReadInt32Async(Stream stream, CancellationToken cancellation)
        {
            byte[] bytes = new byte[sizeof(int)];
            await stream.ReadExactlyAsync(bytes, cancellation);
            return BinaryPrimitives.ReadInt32BigEndian(bytes);
        }

        private static async Task WriteInt32Async(
            Stream stream, int value, CancellationToken cancellation)
        {
            byte[] bytes = new byte[sizeof(int)];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            await stream.WriteAsync(bytes, cancellation);
        }

        private static async Task RelayAsync(
            NetworkStream client, NetworkStream worker, Process child, CancellationToken cancellation)
        {
            using var relay = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
            // Raw streams are essential: buffering short control messages can deadlock Spark.
            Task input = client.CopyToAsync(worker, 65536, relay.Token);
            Task output = worker.CopyToAsync(client, 65536, relay.Token);
            Task exited = child.WaitForExitAsync(relay.Token);
            try
            {
                Task first = await Task.WhenAny(input, output, exited);
                if (first != output)
                {
                    // A process exit may precede the final exception bytes reaching the JVM.
                    await output.WaitAsync(TimeSpan.FromSeconds(2), cancellation);
                }
                else
                {
                    await output;
                }
            }
            finally
            {
                relay.Cancel();
                await ObserveAsync(Task.WhenAll(input, output, exited));
            }
        }

        private static async Task ObserveAsync(Task task)
        {
            try
            {
                await task;
            }
            catch (Exception)
            {
                // The connection owner reports failure and owns cancellation/cleanup.
            }
        }
    }
}
#endif
