// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#if NET8_0_OR_GREATER
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    /// <summary>
    /// Exercises the daemon's public process and socket protocol without a Spark JVM.
    /// Task execution and reuse are covered by the Spark 4 compatibility E2E tests.
    /// </summary>
    public sealed class Spark40DaemonWorkerTests
    {
        private static readonly TimeSpan s_timeout = TimeSpan.FromSeconds(20);

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AcceptsDaemonArgumentsAndReportsDistinctLiveChildPids(bool includeWorkerModule)
        {
            await using var daemon = await DaemonProcess.StartAsync(includeWorkerModule);
            using WorkerConnection first = await daemon.ConnectAsync();
            using WorkerConnection second = await daemon.ConnectAsync();

            Assert.NotEqual(first.Process.Id, second.Process.Id);
            Assert.NotEqual(daemon.Process.Id, first.Process.Id);
            Assert.NotEqual(daemon.Process.Id, second.Process.Id);
            Assert.False(first.Process.HasExited);
            Assert.False(second.Process.HasExited);

            await first.AuthenticateAsync(daemon.Secret);
            await second.AuthenticateAsync(daemon.Secret);
            Assert.False(first.Process.HasExited);
            Assert.False(second.Process.HasExited);

            daemon.CloseInput();
            await daemon.AssertStoppedAsync();
        }

        [Fact]
        public async Task RejectsWrongSecretAndAcceptsReplacement()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection rejected = await daemon.ConnectAsync();

            await WriteStringAsync(rejected.Stream, new string('x', daemon.Secret.Length));
            Assert.Equal("err", await ReadStringAsync(rejected.Stream));
            await AssertClosedAsync(rejected.Stream);
            await AssertExitedAsync(rejected.Process);

            using WorkerConnection replacement = await daemon.ConnectAuthenticatedAsync();
            Assert.False(replacement.Process.HasExited);
            Assert.False(daemon.Process.HasExited);
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(0)]
        [InlineData(int.MaxValue)]
        public async Task RejectsInvalidAuthenticationLengthAndAcceptsReplacement(int length)
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection rejected = await daemon.ConnectAsync();

            await WriteInt32Async(rejected.Stream, length);
            await AssertClosedAsync(rejected.Stream, allowAuthenticationError: true);
            await AssertExitedAsync(rejected.Process);

            using WorkerConnection replacement = await daemon.ConnectAuthenticatedAsync();
            Assert.False(replacement.Process.HasExited);
        }

        [Fact]
        public async Task RejectsTruncatedAuthenticationAndAcceptsReplacement()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection rejected = await daemon.ConnectAsync();

            byte[] secretBytes = Encoding.UTF8.GetBytes(daemon.Secret);
            await WriteInt32Async(rejected.Stream, secretBytes.Length);
            await rejected.Stream.WriteAsync(secretBytes.AsMemory(0, 2)).AsTask().WaitAsync(s_timeout);
            rejected.Client.Client.Shutdown(SocketShutdown.Send);
            await AssertClosedAsync(rejected.Stream, allowAuthenticationError: true);
            await AssertExitedAsync(rejected.Process);

            using WorkerConnection replacement = await daemon.ConnectAuthenticatedAsync();
            Assert.False(replacement.Process.HasExited);
        }

        [Fact]
        public async Task TimesOutUnauthenticatedWorkerAndAcceptsReplacement()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection stalled = await daemon.ConnectAsync();

            // The daemon must retire this connection without receiving any authentication bytes.
            await AssertClosedAsync(stalled.Stream, allowAuthenticationError: true);
            await AssertExitedAsync(stalled.Process);

            using WorkerConnection replacement = await daemon.ConnectAuthenticatedAsync();
            Assert.False(replacement.Process.HasExited);
            Assert.False(daemon.Process.HasExited);
        }

        [Fact]
        public async Task FragmentedStopCancelsOnlyTheTargetWorker()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection target = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection survivor = await daemon.ConnectAuthenticatedAsync();
            await StartBlockedDelegateAsync(target);

            byte[] stop = Int32Bytes(target.Process.Id);
            await daemon.WriteInputAsync(stop.AsMemory(0, 1));

            // A new authenticated connection is a barrier proving that a short stdin read
            // did not shut down the daemon before the remaining PID bytes arrived.
            using WorkerConnection probe = await daemon.ConnectAuthenticatedAsync();
            Assert.False(target.Process.HasExited);
            await daemon.WriteInputAsync(stop.AsMemory(1));

            await AssertExitedAsync(target.Process);
            await AssertClosedAsync(target.Stream);
            Assert.False(survivor.Process.HasExited);
            Assert.False(probe.Process.HasExited);
            Assert.False(daemon.Process.HasExited);

            await daemon.WriteInputAsync(stop);
            using WorkerConnection replacement = await daemon.ConnectAuthenticatedAsync();
            Assert.False(replacement.Process.HasExited);
        }

        [Fact]
        public async Task IgnoresUnknownAndForeignProcessIds()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            await using var foreignDaemon = await DaemonProcess.StartAsync();
            using WorkerConnection ownWorker = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection foreignWorker = await foreignDaemon.ConnectAuthenticatedAsync();

            // Use another owned test process, never the test runner, as the foreign PID oracle.
            await daemon.WriteInputAsync(Int32Bytes(foreignDaemon.Process.Id));
            await daemon.WriteInputAsync(Int32Bytes(int.MaxValue));

            using WorkerConnection ownProbe = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection foreignProbe = await foreignDaemon.ConnectAuthenticatedAsync();
            Assert.False(ownWorker.Process.HasExited);
            Assert.False(foreignWorker.Process.HasExited);
            Assert.False(foreignDaemon.Process.HasExited);
        }

        [Theory]
        [InlineData("eof")]
        [InlineData("truncated")]
        [InlineData("negative")]
        public async Task StdinTerminationStopsAuthenticatedAndAuthenticatingWorkers(string termination)
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection first = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection second = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection authenticating = await daemon.ConnectAsync();

            if (termination == "negative")
            {
                await daemon.WriteInputAsync(Int32Bytes(-1));
            }
            else
            {
                if (termination == "truncated")
                {
                    await daemon.WriteInputAsync(Int32Bytes(first.Process.Id).AsMemory(0, 2));
                }

                daemon.CloseInput();
            }

            await daemon.AssertStoppedAsync();
            await AssertClosedAsync(first.Stream);
            await AssertClosedAsync(second.Stream);
            await AssertClosedAsync(authenticating.Stream, allowAuthenticationError: true);
        }

        [Fact]
        public async Task AbruptDaemonDeathStopsItsChildren()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection first = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection second = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection authenticating = await daemon.ConnectAsync();
            await StartBlockedDelegateAsync(first);

            // Deliberately kill only the daemon. Killing the process tree would hide orphans.
            daemon.Process.Kill();
            await daemon.AssertStoppedAsync();
            await AssertClosedAsync(first.Stream);
            await AssertClosedAsync(second.Stream);
            await AssertClosedAsync(authenticating.Stream, allowAuthenticationError: true);
        }

        [LinuxFact]
        public async Task SigtermStopsDaemonAndItsChildren()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection first = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection second = await daemon.ConnectAuthenticatedAsync();

            var startInfo = new ProcessStartInfo("/bin/kill")
            {
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            startInfo.ArgumentList.Add("-TERM");
            startInfo.ArgumentList.Add("--");
            startInfo.ArgumentList.Add(daemon.Process.Id.ToString(CultureInfo.InvariantCulture));
            using Process signal = Process.Start(startInfo);
            await AssertExitedAsync(signal);
            Assert.Equal(0, signal.ExitCode);

            await daemon.AssertStoppedAsync();
            await AssertClosedAsync(first.Stream);
            await AssertClosedAsync(second.Stream);
        }

        [Fact]
        public async Task WorkerProcessFailureDoesNotStopOtherWorkersOrTheDaemon()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection failed = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection survivor = await daemon.ConnectAuthenticatedAsync();

            failed.Process.Kill();
            await AssertExitedAsync(failed.Process);
            await AssertClosedAsync(failed.Stream);

            using WorkerConnection replacement = await daemon.ConnectAuthenticatedAsync();
            Assert.False(survivor.Process.HasExited);
            Assert.False(replacement.Process.HasExited);
            Assert.False(daemon.Process.HasExited);
        }

        [Fact]
        public async Task JvmDisconnectRetiresItsWorker()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection worker = await daemon.ConnectAuthenticatedAsync();

            worker.Client.Dispose();
            await AssertExitedAsync(worker.Process);

            using WorkerConnection replacement = await daemon.ConnectAuthenticatedAsync();
            Assert.False(replacement.Process.HasExited);
        }

        [Fact]
        public async Task FinalExceptionCanBeReadAfterWorkerExit()
        {
            await using var daemon = await DaemonProcess.StartAsync();
            using WorkerConnection worker = await daemon.ConnectAuthenticatedAsync();
            const int MessageSize = 128 * 1024;
            byte[] frame = CreateLifecycleTask(blockDelegate: false, MessageSize);
            await worker.Stream.WriteAsync(frame).AsTask().WaitAsync(s_timeout);

            Assert.Equal((int)SpecialLengths.PYTHON_EXCEPTION_THROWN, await ReadInt32Async(worker.Stream));
            int length = await ReadInt32Async(worker.Stream);
            Assert.InRange(length, MessageSize, MessageSize + 16 * 1024);

            // Pause the reader until the actual worker process exits. The daemon must still
            // deliver the exception's complete suffix, even when exit wins the relay race.
            await AssertExitedAsync(worker.Process);
            var bytes = new byte[length];
            for (int offset = 0; offset < length;)
            {
                int chunk = Math.Min(1024, length - offset);
                await worker.Stream.ReadExactlyAsync(bytes.AsMemory(offset, chunk)).AsTask().WaitAsync(s_timeout);
                offset += chunk;
                await Task.Yield();
            }

            string exception = Encoding.UTF8.GetString(bytes);
            Assert.Contains("WI03_EXCEPTION_BEGIN" + new string('x', MessageSize) + "WI03_EXCEPTION_END", exception);
            await AssertClosedAsync(worker.Stream);

            using WorkerConnection replacement = await daemon.ConnectAuthenticatedAsync();
            Assert.False(replacement.Process.HasExited);
        }

        [Fact]
        public async Task UnreadDaemonStderrDoesNotBlockTaskOrShutdown()
        {
            await using var daemon = await DaemonProcess.StartAsync(readStandardError: false);
            using WorkerConnection noisy = await daemon.ConnectAuthenticatedAsync();
            using WorkerConnection sibling = await daemon.ConnectAuthenticatedAsync();

            // Exceed the stderr pipe's capacity without ever reading it. The task's socket
            // marker is emitted only after its diagnostic write has completed.
            await StartBlockedDelegateAsync(noisy, diagnosticBytes: 1024 * 1024);
            Assert.False(noisy.Process.HasExited);
            Assert.False(sibling.Process.HasExited);

            daemon.CloseInput();
            await daemon.AssertStoppedAsync();
            await AssertClosedAsync(noisy.Stream);
            await AssertClosedAsync(sibling.Stream);
        }

        [Fact]
        public async Task RelayStopsWithinDeadlineWhenWorkerExitsAndJvmStopsReading()
        {
            // This is a relay-boundary integration check: real sockets and an OS process,
            // with the private seam used to make downstream backpressure deterministic.
            await using var process = await DaemonProcess.StartAsync();
            using SocketPair jvm = await SocketPair.CreateAsync();
            using SocketPair worker = await SocketPair.CreateAsync();
            using var timeout = new CancellationTokenSource(s_timeout);
            MethodInfo method = typeof(Spark40DaemonWorker).GetMethod(
                "RelayAsync", BindingFlags.NonPublic | BindingFlags.Static);
            Assert.NotNull(method);
            Task relay = (Task)method.Invoke(null, new object[]
            {
                jvm.Local.GetStream(), worker.Local.GetStream(), process.Process, timeout.Token,
            });
            Task producer = ProduceRelayOutputAsync(worker.Peer, timeout.Token);
            try
            {
                var firstByte = new byte[1];
                await jvm.Peer.GetStream().ReadExactlyAsync(firstByte, timeout.Token);
                Assert.Equal((byte)0x5a, firstByte[0]);
                Assert.False(producer.IsCompleted, "The test peer did not apply output backpressure.");
                Assert.False(relay.IsCompleted);

                // Leave the JVM's input and output sides open, but do not read any more bytes.
                var elapsed = Stopwatch.StartNew();
                process.Process.Kill();
                await AssertExitedAsync(process.Process);
                await Assert.ThrowsAsync<TimeoutException>(async () =>
                    await relay.WaitAsync(TimeSpan.FromSeconds(10)));
                Assert.True(relay.IsCompleted, "Only the test's outer timeout expired; the relay is still running.");
                Assert.InRange(elapsed.Elapsed.TotalSeconds, 1, 8);
            }
            finally
            {
                timeout.Cancel();
                jvm.Dispose();
                worker.Dispose();
                try
                {
                    await Task.WhenAll(relay, producer).WaitAsync(s_timeout);
                }
                catch (Exception exception) when (
                    exception is OperationCanceledException || exception is IOException ||
                    exception is TimeoutException || exception is ObjectDisposedException)
                {
                    // Both copies are observed after closing the test-owned sockets.
                }
            }
        }

        private static async Task ProduceRelayOutputAsync(TcpClient peer, CancellationToken cancellation)
        {
            var bytes = new byte[64 * 1024];
            Array.Fill(bytes, (byte)0x5a);
            for (int i = 0; i < 1024; ++i)
            {
                await peer.GetStream().WriteAsync(bytes, cancellation);
            }

            // If all data fits in the OS buffers, expose EOF so the test cannot mistake
            // a worker that never closes its output for downstream backpressure.
            peer.Client.Shutdown(SocketShutdown.Send);
        }

        private static async Task StartBlockedDelegateAsync(WorkerConnection worker, int diagnosticBytes = 0)
        {
            byte[] frame = CreateLifecycleTask(blockDelegate: true, messageSize: diagnosticBytes);
            await worker.Stream.WriteAsync(frame).AsTask().WaitAsync(s_timeout);
            Assert.Equal(1234567, await ReadInt32Async(worker.Stream));
        }

        private static byte[] CreateLifecycleTask(bool blockDelegate, int messageSize)
        {
            using var stream = new MemoryStream();
            Payload payload = TestData.GetDefaultPayload();
            SerDe.Write(stream, payload.SplitIndex);
            SerDe.Write(stream, payload.Version);
            new TaskContextWriterV3_3_X().Write(stream, payload.TaskContext);
            SerDe.Write(stream, AppContext.BaseDirectory);
            SerDe.Write(stream, 0); // Included files.
            SerDe.Write(stream, false); // Broadcast decryption.
            SerDe.Write(stream, 0); // Broadcast variables.
            SerDe.Write(stream, (int)UdfUtils.PythonEvalType.NON_UDF);
            var wrapper = new RawUdfWrapper(ExecuteLifecycleTask);
            byte[] command = CommandSerDe.Serialize((RawWorkerFunction.ExecuteDelegate)wrapper.Execute);
            SerDe.Write(stream, command.Length);
            SerDe.Write(stream, command);
            SerDe.Write(stream, blockDelegate);
            SerDe.Write(stream, messageSize);
            return stream.ToArray();
        }

        private static int ExecuteLifecycleTask(
            int splitId,
            Stream input,
            Stream output,
            CommandSerDe.SerializedMode serializer,
            CommandSerDe.SerializedMode deserializer)
        {
            bool blockDelegate = SerDe.ReadBool(input);
            int messageSize = SerDe.ReadInt32(input);
            if (blockDelegate)
            {
                if (messageSize > 0)
                {
                    Console.Error.Write(new string('x', messageSize));
                    Console.Error.Flush();
                }

                SerDe.Write(output, 1234567);
                output.Flush();
                Thread.Sleep(Timeout.Infinite);
                return 0;
            }

            throw new InvalidOperationException(
                "WI03_EXCEPTION_BEGIN" + new string('x', messageSize) + "WI03_EXCEPTION_END");
        }

        private static byte[] Int32Bytes(int value)
        {
            var bytes = new byte[sizeof(int)];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            return bytes;
        }

        private static async Task WriteInt32Async(Stream stream, int value)
        {
            await stream.WriteAsync(Int32Bytes(value)).AsTask().WaitAsync(s_timeout);
        }

        private static async Task<int> ReadInt32Async(Stream stream)
        {
            var bytes = new byte[sizeof(int)];
            await stream.ReadExactlyAsync(bytes).AsTask().WaitAsync(s_timeout);
            return BinaryPrimitives.ReadInt32BigEndian(bytes);
        }

        private static async Task WriteStringAsync(Stream stream, string value)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            await WriteInt32Async(stream, bytes.Length);
            await stream.WriteAsync(bytes).AsTask().WaitAsync(s_timeout);
        }

        private static async Task<string> ReadStringAsync(Stream stream)
        {
            int length = await ReadInt32Async(stream);
            Assert.InRange(length, 0, 32);
            var bytes = new byte[length];
            await stream.ReadExactlyAsync(bytes).AsTask().WaitAsync(s_timeout);
            return Encoding.UTF8.GetString(bytes);
        }

        private static async Task AssertClosedAsync(Stream stream, bool allowAuthenticationError = false)
        {
            var bytes = new byte[16];
            int totalRead = 0;
            using var timeout = new CancellationTokenSource(s_timeout);
            try
            {
                int read;
                while ((read = await stream.ReadAsync(bytes, timeout.Token)) != 0)
                {
                    totalRead += read;
                    Assert.True(allowAuthenticationError, "Unexpected data before the socket closed.");
                    Assert.InRange(totalRead, 0, sizeof(int) + 3);
                }
            }
            catch (IOException)
            {
                // Closing a rejected/cancelled connection can produce either EOF or reset.
            }
        }

        private static async Task AssertExitedAsync(Process process)
        {
            using var timeout = new CancellationTokenSource(s_timeout);
            await process.WaitForExitAsync(timeout.Token);
            Assert.True(process.HasExited, $"Process {process.Id} is still alive.");
        }

        private sealed class LinuxFactAttribute : FactAttribute
        {
            public LinuxFactAttribute()
            {
                if (!OperatingSystem.IsLinux())
                {
                    Skip = "SIGTERM process behavior is only exercised on Linux.";
                }
            }
        }

        private sealed class WorkerConnection : IDisposable
        {
            internal WorkerConnection(TcpClient client, Process process)
            {
                Client = client;
                Process = process;
                Stream = client.GetStream();
            }

            internal TcpClient Client { get; }

            internal Process Process { get; }

            internal NetworkStream Stream { get; }

            internal async Task AuthenticateAsync(string secret)
            {
                await WriteStringAsync(Stream, secret);
                Assert.Equal("ok", await ReadStringAsync(Stream));
            }

            public void Dispose() => Client.Dispose();
        }

        private sealed class SocketPair : IDisposable
        {
            private SocketPair(TcpClient local, TcpClient peer)
            {
                Local = local;
                Peer = peer;
            }

            internal TcpClient Local { get; }

            internal TcpClient Peer { get; }

            internal static async Task<SocketPair> CreateAsync()
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                var peer = new TcpClient
                {
                    NoDelay = true,
                    SendBufferSize = 1024,
                    ReceiveBufferSize = 1024,
                };
                try
                {
                    listener.Start();
                    await peer.ConnectAsync(
                        IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port).WaitAsync(s_timeout);
                    TcpClient local = await listener.AcceptTcpClientAsync().WaitAsync(s_timeout);
                    local.NoDelay = true;
                    local.SendBufferSize = 1024;
                    local.ReceiveBufferSize = 1024;
                    return new SocketPair(local, peer);
                }
                catch
                {
                    peer.Dispose();
                    throw;
                }
                finally
                {
                    listener.Stop();
                }
            }

            public void Dispose()
            {
                Local.Dispose();
                Peer.Dispose();
            }
        }

        private sealed class DaemonProcess : IAsyncDisposable
        {
            private readonly List<Process> _children = new List<Process>();
            private readonly List<TcpClient> _connections = new List<TcpClient>();
            private Task<string> _stderr;
            private Task<string> _stdout;
            private int _port;

            private DaemonProcess(Process process, string secret)
            {
                Process = process;
                Secret = secret;
            }

            internal Process Process { get; }

            internal string Secret { get; }

            internal static async Task<DaemonProcess> StartAsync(
                bool includeWorkerModule = true, bool readStandardError = true)
            {
                string workerAssembly = Path.Combine(AppContext.BaseDirectory, "Microsoft.Spark.Worker.dll");
                Assert.True(File.Exists(workerAssembly), $"Worker assembly was not copied to {workerAssembly}.");
                Assert.True(File.Exists(Path.ChangeExtension(workerAssembly, ".runtimeconfig.json")));

                string host = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH");
                if (string.IsNullOrEmpty(host))
                {
                    string currentHost = Environment.ProcessPath;
                    host = string.Equals(
                        Path.GetFileNameWithoutExtension(currentHost),
                        "dotnet",
                        StringComparison.OrdinalIgnoreCase) ? currentHost : "dotnet";
                }

                var startInfo = new ProcessStartInfo(host)
                {
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    WorkingDirectory = AppContext.BaseDirectory,
                };
                startInfo.ArgumentList.Add(workerAssembly);
                startInfo.ArgumentList.Add("-m");
                startInfo.ArgumentList.Add("pyspark.daemon");
                if (includeWorkerModule)
                {
                    startInfo.ArgumentList.Add("pyspark.worker");
                }

                string secret = Guid.NewGuid().ToString("N");
                startInfo.Environment["DOTNET_WORKER_SPARK_VERSION"] = "4.0.4";
                startInfo.Environment["PYTHON_WORKER_FACTORY_SECRET"] = secret;
                startInfo.Environment["SPARK_REUSE_WORKER"] = "1";
                startInfo.Environment["DOTNET_WORKER_DEBUG"] = "0";

                var daemon = new DaemonProcess(new Process { StartInfo = startInfo }, secret);
                try
                {
                    Assert.True(daemon.Process.Start());
                    daemon._stderr = readStandardError ?
                        daemon.Process.StandardError.ReadToEndAsync() : Task.FromResult(string.Empty);
                    daemon._port = await ReadInt32Async(daemon.Process.StandardOutput.BaseStream);
                    Assert.InRange(daemon._port, 1, ushort.MaxValue);
                    daemon._stdout = daemon.Process.StandardOutput.ReadToEndAsync();
                    return daemon;
                }
                catch (Exception exception)
                {
                    await daemon.DisposeAsync();
                    string stderr = daemon._stderr == null ? string.Empty : await daemon._stderr.WaitAsync(s_timeout);
                    throw new InvalidOperationException($"Daemon startup failed. {stderr}", exception);
                }
            }

            internal async Task<WorkerConnection> ConnectAsync()
            {
                var client = new TcpClient { NoDelay = true };
                _connections.Add(client);
                await client.ConnectAsync(IPAddress.Loopback, _port).WaitAsync(s_timeout);
                int pid = await ReadInt32Async(client.GetStream());
                Assert.True(pid > 0, $"Daemon returned invalid worker PID {pid}.");
                Assert.NotEqual(Process.Id, pid);

                Process child = Process.GetProcessById(pid);
                _children.Add(child);
                Assert.False(child.HasExited, $"Advertised worker PID {pid} is not alive.");
                return new WorkerConnection(client, child);
            }

            internal async Task<WorkerConnection> ConnectAuthenticatedAsync()
            {
                WorkerConnection connection = await ConnectAsync();
                await connection.AuthenticateAsync(Secret);
                return connection;
            }

            internal async Task WriteInputAsync(ReadOnlyMemory<byte> bytes)
            {
                await Process.StandardInput.BaseStream.WriteAsync(bytes).AsTask().WaitAsync(s_timeout);
                await Process.StandardInput.BaseStream.FlushAsync().WaitAsync(s_timeout);
            }

            internal void CloseInput() => Process.StandardInput.Close();

            internal async Task AssertStoppedAsync()
            {
                await AssertExitedAsync(Process);
                foreach (Process child in _children)
                {
                    await AssertExitedAsync(child);
                }

                Assert.Equal(string.Empty, await _stdout.WaitAsync(s_timeout));
                await _stderr.WaitAsync(s_timeout);
            }

            public async ValueTask DisposeAsync()
            {
                foreach (TcpClient connection in _connections)
                {
                    connection.Dispose();
                }

                try
                {
                    CloseInput();
                }
                catch (InvalidOperationException)
                {
                    // Process startup itself may have failed.
                }

                try
                {
                    await StopProcessAsync(Process, entireProcessTree: true);
                }
                finally
                {
                    foreach (Process child in _children)
                    {
                        try
                        {
                            await StopProcessAsync(child, entireProcessTree: true);
                        }
                        finally
                        {
                            child.Dispose();
                        }
                    }

                    Process.Dispose();
                }
            }

            private static async Task StopProcessAsync(Process process, bool entireProcessTree)
            {
                try
                {
                    if (!process.HasExited)
                    {
                        try
                        {
                            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(3));
                        }
                        catch (TimeoutException)
                        {
                            if (!process.HasExited)
                            {
                                process.Kill(entireProcessTree);
                            }
                        }
                    }

                    await AssertExitedAsync(process);
                }
                catch (InvalidOperationException)
                {
                    // A failed Process.Start has no OS process to clean up.
                }
            }
        }
    }
}
#endif
