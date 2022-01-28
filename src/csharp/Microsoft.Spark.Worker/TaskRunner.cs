// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Services;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Command;
using Microsoft.Spark.Worker.Processor;
using Microsoft.Spark.Worker.Utils;
using static Microsoft.Spark.Utils.AssemblyInfoProvider;

namespace Microsoft.Spark.Worker
{
    /// <summary>
    /// TaskRunner is used to run Spark task assigned by JVM side. It uses a TCP socket to
    /// communicate with JVM side. This socket may be reused to run multiple Spark tasks.
    /// </summary>
    internal class TaskRunner
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(TaskRunner));

        private readonly ISocketWrapper _socket;

        private volatile bool _isRunning = false;

        private int _numTasksRun = 0;

        private readonly bool _reuseSocket;

        private readonly Version _version;

        public TaskRunner(int taskId, ISocketWrapper socket, bool reuseSocket, Version version)
        {
            TaskId = taskId;
            _socket = socket;
            _reuseSocket = reuseSocket;
            _version = version;
        }

        public int TaskId { get; }

        public void Run()
        {
            s_logger.LogInfo($"[{TaskId}] Starting with ReuseSocket[{_reuseSocket}].");

            if (EnvironmentUtils.GetEnvironmentVariableAsBool("DOTNET_WORKER_DEBUG"))
            {
                Debugger.Launch();
            }

            _isRunning = true;
            Stream inputStream = _socket.InputStream;
            Stream outputStream = _socket.OutputStream;

            try
            {
                while (_isRunning)
                {
                    Payload payload = ProcessStream(
                        inputStream,
                        outputStream,
                        _version,
                        out bool readComplete);

                    if (payload != null)
                    {
                        outputStream.Flush();

                        ++_numTasksRun;

                        // If the socket is not read through completely, then it cannot be reused.
                        if (!readComplete)
                        {
                            _isRunning = false;

                            // Wait for server to complete to avoid 'connection reset' exception.
                            s_logger.LogInfo($"[{TaskId}] Sleep 500 millisecond to close socket.");
                            Thread.Sleep(500);
                        }
                        else if (!_reuseSocket)
                        {
                            _isRunning = false;

                            // Use SerDe.ReadBytes() to detect Java side has closed socket
                            // properly. SerDe.ReadBytes() will block until the socket is closed.
                            s_logger.LogInfo($"[{TaskId}] Waiting for JVM side to close socket.");
                            SerDe.ReadBytes(inputStream);
                            s_logger.LogInfo($"[{TaskId}] JVM side has closed socket.");
                        }
                    }
                    else
                    {
                        _isRunning = false;
                        s_logger.LogWarn(
                            $"[{TaskId}] Read null payload. Socket is closed by JVM.");
                    }
                }
            }
            catch (Exception e)
            {
                _isRunning = false;
                s_logger.LogError($"[{TaskId}] Exiting with exception: {e}");
            }
            finally
            {
                try
                {
                    _socket.Dispose();
                }
                catch (Exception ex)
                {
                    s_logger.LogWarn($"[{TaskId}] Exception while closing socket: {ex}");
                }

                s_logger.LogInfo($"[{TaskId}] Finished running {_numTasksRun} task(s).");
            }
        }

        public void Stop()
        {
            _isRunning = false;
            s_logger.LogInfo($"Stopping TaskRunner [{TaskId}]");
        }

        private Payload ProcessStream(
            Stream inputStream,
            Stream outputStream,
            Version version,
            out bool readComplete)
        {
            readComplete = false;

            try
            {
                DateTime bootTime = DateTime.UtcNow;

                Payload payload = new PayloadProcessor(version).Process(inputStream);
                if (payload is null)
                {
                    return null;
                }

                ValidateVersion(payload.Version);

                DateTime initTime = DateTime.UtcNow;

                CommandExecutorStat commandExecutorStat = new CommandExecutor(version).Execute(
                    inputStream,
                    outputStream,
                    payload.SplitIndex,
                    payload.Command);

                DateTime finishTime = DateTime.UtcNow;

                WriteDiagnosticsInfo(outputStream, bootTime, initTime, finishTime);

                // Mark the beginning of the accumulators section of the output
                SerDe.Write(outputStream, (int)SpecialLengths.END_OF_DATA_SECTION);

                // TODO: Extend the following to write accumulator values here.
                SerDe.Write(outputStream, 0);

                // Check the end of stream.
                int endOfStream = SerDe.ReadInt32(inputStream);
                if (endOfStream == (int)SpecialLengths.END_OF_STREAM)
                {
                    s_logger.LogDebug($"[{TaskId}] Received END_OF_STREAM signal.");

                    SerDe.Write(outputStream, (int)SpecialLengths.END_OF_STREAM);
                    readComplete = true;
                }
                else
                {
                    // This may happen when the input data is not read completely,
                    // e.g., when take() operation is performed
                    s_logger.LogWarn($"[{TaskId}] Unexpected end of stream: {endOfStream}.");
                    s_logger.LogWarn(SerDe.ReadInt32(inputStream).ToString());

                    // Write a different value to tell JVM to not reuse this worker.
                    SerDe.Write(outputStream, (int)SpecialLengths.END_OF_DATA_SECTION);
                }

                LogStat(commandExecutorStat, readComplete);

                return payload;
            }
            catch (Exception e)
            {
                s_logger.LogError($"[{TaskId}] ProcessStream() failed with exception: {e}");

                try
                {
                    SerDe.Write(outputStream, (int)SpecialLengths.PYTHON_EXCEPTION_THROWN);
                    SerDe.Write(outputStream, e.ToString());
                }
                catch (IOException)
                {
                    // JVM closed the socket.
                }
                catch (Exception ex)
                {
                    s_logger.LogError(
                        $"[{TaskId}] Writing exception to stream failed with exception: {ex}");
                }

                throw;
            }
        }

        private void ValidateVersion(string driverMicrosoftSparkVersion)
        {
            string workerVersion = MicrosoftSparkWorkerAssemblyInfo().AssemblyVersion;
            // Worker is compatibile only within the same major version.
            if (new Version(driverMicrosoftSparkVersion).Major != new Version(workerVersion).Major)
            {
                throw new Exception("The major version of Microsoft.Spark.Worker " +
                    $"({workerVersion}) does not match with Microsoft.Spark " +
                    $"({driverMicrosoftSparkVersion}) on the driver.");
            }
        }

        private void LogStat(CommandExecutorStat stat, bool readComplete)
        {
            s_logger.LogInfo($"[{TaskId}] Processed a task: readComplete:{readComplete}, entries:{stat.NumEntriesProcessed}");
        }

        private void WriteDiagnosticsInfo(
            Stream stream,
            DateTime bootTime,
            DateTime initTime,
            DateTime finishTime)
        {
            SerDe.Write(stream, (int)SpecialLengths.TIMING_DATA);
            SerDe.Write(stream, bootTime.ToUnixTime());
            SerDe.Write(stream, initTime.ToUnixTime());
            SerDe.Write(stream, finishTime.ToUnixTime());

            SerDe.Write(stream, 0L); // shuffle.MemoryBytesSpilled  
            SerDe.Write(stream, 0L); // shuffle.DiskBytesSpilled
        }
    }
}
