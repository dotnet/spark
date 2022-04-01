// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Services;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Worker
{
    /// <summary>
    /// DaemonWorker provides functionalities equivalent to PySpark's daemon server.
    /// Refer to Spark's PythonWorkerFactory.scala for how Spark creates daemon
    /// server and interacts with it.
    /// </summary>
    internal sealed class DaemonWorker
    {
        private static ILoggerService s_logger = null;

        /// <summary>
        /// Keeps track of all the TaskRunner objects identified by the its Id.
        /// The main thread creates a TaskRunner each time it receives a new socket
        /// connection from JVM side and inserts it into _taskRunners, whereas
        /// each worker thread removes from it when TaskRunner.Run() is finished, thus
        /// _taskRunners is using ConcurrentDictionary.
        /// Also, _taskRunners is used to bound the number of worker threads since it
        /// gives you the total number of active TaskRunners.
        /// </summary>
        private readonly ConcurrentDictionary<int, TaskRunner> _taskRunners =
            new ConcurrentDictionary<int, TaskRunner>();

        /// <summary>
        /// Each worker thread picks up the TaskRunner from _waitingTaskRunners
        /// and runs it.
        /// </summary>
        private readonly BlockingCollection<TaskRunner> _waitingTaskRunners =
            new BlockingCollection<TaskRunner>();

        private readonly Version _version;

        internal DaemonWorker(Version version)
        {
            _version = version;
        }

        internal int CurrentNumTaskRunners => _taskRunners.Count();

        /// <summary>
        /// Runs the DaemonWorker server.
        /// </summary>
        internal void Run()
        {
            // TODO: Note that daemon server is stopped from Spark, it is done with
            // Process.destroy(). It should send SIGTERM and the signal should be handled
            // by the following:
            // AppDomain.CurrentDomain.ProcessExit += (s, e) => {};,
            // but the above handler is not invoked. This can be investigated if more
            // graceful exit is required.
            ISocketWrapper listener = SocketFactory.CreateSocket();
            Run(listener);
        }

        internal void Run(ISocketWrapper listener)
        {
            try
            {
                listener.Listen();

                // Communicate the server port back to the Spark using standard output.
                Stream outputStream = Console.OpenStandardOutput();
                var serverPort = ((IPEndPoint)listener.LocalEndPoint).Port;
                SerDe.Write(outputStream, serverPort);

                // Now the logger can be initialized after standard output's usage is done.
                s_logger = LoggerServiceFactory.GetLogger(typeof(DaemonWorker));

                s_logger.LogInfo($"Started .NET DaemonServer with port {serverPort}.");

                // Start accepting connections from JVM.
                new Thread(() => { StartServer(listener); }).Start();

                WaitForSignal();
            }
            catch (Exception e)
            {
                s_logger.LogError($".NET DaemonWorker is exiting with exception: {e}.");
                Environment.Exit(-1);
            }
            finally
            {
                _waitingTaskRunners.Dispose();
            }
        }

        /// <summary>
        /// Starts listening to any connection from JVM.
        /// </summary>
        private void StartServer(ISocketWrapper listener)
        {
            try
            {
                bool reuseWorker =
                    "1".Equals(Environment.GetEnvironmentVariable("SPARK_REUSE_WORKER"));

                string secret = Utils.SettingUtils.GetWorkerFactorySecret();

                int taskRunnerId = 1;
                int numWorkerThreads = 0;

                while (true)
                {
                    ISocketWrapper socket = listener.Accept();
                    s_logger.LogInfo($"New connection accepted for TaskRunner [{taskRunnerId}]");

                    bool authStatus = true;
                    if (!string.IsNullOrWhiteSpace(secret))
                    {
                        // The Spark side expects the PID from a forked process.
                        // In .NET implementation, a task runner id is used instead.
                        SerDe.Write(socket.OutputStream, taskRunnerId);
                        socket.OutputStream.Flush();

                        if (ConfigurationService.IsDatabricks)
                        {
                            SerDe.ReadString(socket.InputStream);
                        }

                        authStatus = Authenticator.AuthenticateAsServer(socket, secret);
                    }

                    if (authStatus)
                    {
                        var taskRunner = new TaskRunner(
                            taskRunnerId,
                            socket,
                            reuseWorker,
                            _version);

                        _waitingTaskRunners.Add(taskRunner);
                        _taskRunners[taskRunnerId] = taskRunner;

                        ++taskRunnerId;

                        // When reuseWorker is set to true, numTaskRunners will be always one
                        // greater than numWorkerThreads since TaskRunner.Run() does not return
                        // so that the task runner object is not removed from _taskRunners.
                        int numTaskRunners = CurrentNumTaskRunners;

                        while (numWorkerThreads < numTaskRunners)
                        {
                            // Note that in the current implementation of RunWorkerThread() does
                            // not return. If more graceful exit is required, RunWorkerThread() can
                            // be updated to return upon receiving a signal from this main thread.
                            new Thread(RunWorkerThread).Start();
                            ++numWorkerThreads;
                        }

                        s_logger.LogInfo(
                            $"Pool snapshot: [NumThreads:{numWorkerThreads}], [NumTaskRunners:{numTaskRunners}]");
                    }
                    else
                    {
                        // Use SerDe.ReadBytes() to detect Java side has closed socket
                        // properly. ReadBytes() will block until the socket is closed.
                        s_logger.LogError(
                            "Authentication failed. Waiting for JVM side to close socket.");
                        SerDe.ReadBytes(socket.InputStream);

                        socket.Dispose();
                    }
                }
            }
            catch (Exception e)
            {
                s_logger.LogError($"StartServer() exits with exception: {e}");
                Environment.Exit(-1);
            }
        }

        /// <summary>
        /// RunWorkerThread() is called for each worker thread when it starts.
        /// RunWorkerThread() doesn't return (except for the error cases), and
        /// keeps pulling from _waitingTaskRunners and runs the retrieved TaskRunner.
        /// </summary>
        private void RunWorkerThread()
        {
            try
            {
                while (true)
                {
                    if (_waitingTaskRunners.TryTake(out TaskRunner taskRunner, Timeout.Infinite))
                    {
                        // Note that if the task runner is marked as "reuse", the following
                        // never returns.
                        taskRunner.Run();

                        // Once the task runner returns (in case of not "reusing"),
                        // remove it from _taskRunners to prevent the main thread from
                        // creating more threads than needed.
                        _taskRunners.TryRemove(taskRunner.TaskId, out TaskRunner tmp);
                    }
                }
            }
            catch (Exception e)
            {
                s_logger.LogError($"RunWorkerThread() exits with an exception: {e}");
                Environment.Exit(-1);
            }
        }

        /// <summary>
        /// The main thread call this to wait for any signals from JVM.
        /// </summary>
        private void WaitForSignal()
        {
            // The main thread keeps reading from standard output to check if there
            // is any signal from JVM to destroy a worker identified by task runner id.
            // The signal is sent only if the work is interrupted. Refer to PythonRunner.scala.
            Stream inputStream = Console.OpenStandardInput();
            while (true)
            {
                var bytes = new byte[sizeof(int)];
                int readBytes = inputStream.Read(bytes, 0, bytes.Length);

                if (readBytes != bytes.Length)
                {
                    s_logger.LogError("Read error, length: {0}, will exit", readBytes);
                    Environment.Exit(-1);
                }

                int taskRunnerId = BinaryPrimitives.ReadInt32BigEndian(bytes);
                if (taskRunnerId < 0)
                {
                    s_logger.LogInfo($"Received negative TaskRunnerId: {taskRunnerId}, will exit");
                    Environment.Exit(0);
                }
                else
                {
                    // TODO: Note that stopping a TaskRunner is not implemented yet and
                    // will be revisited. Note that TaskRunner.Stop() is the best effort to
                    // stop the TaskRunner, which may block waiting for the input stream.
                    // In this case, we can revisit to see if the thread that runs the TaskRunner
                    // can be just "aborted."
                    s_logger.LogInfo($"Ignoring received signal to stop TaskRunner [{taskRunnerId}].");
                }
            }
        }
    }
}
