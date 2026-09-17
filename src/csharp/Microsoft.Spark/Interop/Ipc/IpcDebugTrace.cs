// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// Opt-in, file-only tracing for the E2E hang investigation. Never writes to worker stdout.
    /// </summary>
    internal sealed class IpcDebugTrace
    {
        private const string SizeLimitMarker = "trace-size-limit-reached\n";
        private static readonly IpcDebugTrace s_trace = new IpcDebugTrace(
            Environment.GetEnvironmentVariable("DOTNET_SPARK_DEBUG_TRACE_DIR"));
        private static long s_callId;

        private readonly object _gate = new object();
        private readonly int _processId = Process.GetCurrentProcess().Id;
        private readonly string _path;
        private readonly long _maxBytes;
        private long _bytesWritten;
        private int _dropped;
        private bool _disabled;

        internal IpcDebugTrace(string directory, long maxBytes = 16 * 1024 * 1024)
        {
            _maxBytes = maxBytes;
            if (string.IsNullOrWhiteSpace(directory))
            {
                return;
            }

            try
            {
                Directory.CreateDirectory(directory);
                _path = Path.Combine(directory, $"trace-{_processId}-{Guid.NewGuid():N}.log");
            }
            catch (Exception)
            {
                // A diagnostic path failure must not change the operation being observed.
            }
        }

        internal static void Write(string message) => s_trace.WriteEvent(message);

        internal static long StartCall(string method)
        {
            if (s_trace._path == null)
            {
                return 0;
            }

            long id = Interlocked.Increment(ref s_callId);
            Call(id, method, "wait-semaphore");
            return id;
        }

        internal static void Call(long id, string method, string phase)
        {
            if (id != 0)
            {
                Write($"ipc call={id} method={method} phase={phase}");
            }
        }

        internal void WriteEvent(string message)
        {
            if (_path == null)
            {
                return;
            }

            // Tracing must not introduce another contended wait into the IPC path.
            if (!Monitor.TryEnter(_gate))
            {
                Interlocked.Increment(ref _dropped);
                return;
            }

            try
            {
                if (_disabled)
                {
                    return;
                }

                string line = $"{DateTime.UtcNow:O} pid={_processId} " +
                    $"tid={Thread.CurrentThread.ManagedThreadId} " +
                    $"dropped={Interlocked.Exchange(ref _dropped, 0)} {message}\n";
                int byteCount = Encoding.UTF8.GetByteCount(line);
                if (_bytesWritten + byteCount > _maxBytes - SizeLimitMarker.Length)
                {
                    File.AppendAllText(_path, SizeLimitMarker);
                    _disabled = true;
                    return;
                }

                // Close after each event so evidence survives testhost termination.
                File.AppendAllText(_path, line);
                _bytesWritten += byteCount;
            }
            catch (Exception)
            {
                _disabled = true;
            }
            finally
            {
                Monitor.Exit(_gate);
            }
        }
    }
}
