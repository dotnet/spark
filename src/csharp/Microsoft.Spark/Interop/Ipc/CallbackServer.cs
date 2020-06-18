// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.Network;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// CallbackServer services callback requests from the JVM.
    /// </summary>
    internal sealed class CallbackServer
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(CallbackServer));

        /// <summary>
        /// Keeps track of all <see cref="ICallbackHandler"/>s by its Id. This is accessed
        /// by the <see cref="CallbackServer"/> and the <see cref="CallbackConnection"/>
        /// running in the worker tasks.
        /// </summary>
        private readonly ConcurrentDictionary<int, ICallbackHandler> _callbackHandlers =
            new ConcurrentDictionary<int, ICallbackHandler>();

        /// <summary>
        /// Keeps track of all <see cref="CallbackConnection"/> objects identified by its
        /// <see cref="CallbackConnection.ConnectionId"/>. The main thread creates a
        /// <see cref="CallbackConnection"/> each time it receives a new socket connection
        /// from the JVM side and inserts it into <see cref="_connections"/>. Each worker
        /// task calls <see cref="CallbackConnection.Run"/> and removes the connection
        /// once this call is finished. <see cref="CallbackConnection.Run"/> will not return
        /// unless the <see cref="CallbackConnection"/> needs to be closed.
        /// Also, <see cref="_connections"/> is used to bound the number of worker tasks
        /// since it gives you the total number of active <see cref="CallbackConnection"/>s.
        /// </summary>
        private readonly ConcurrentDictionary<long, CallbackConnection> _connections =
            new ConcurrentDictionary<long, CallbackConnection>();

        /// <summary>
        /// Each worker task picks up a CallbackConnection from _waitingConnections
        /// and runs it.
        /// </summary>
        private readonly BlockingCollection<CallbackConnection> _waitingConnections =
            new BlockingCollection<CallbackConnection>();

        /// <summary>
        /// Maintains a list of all the worker tasks.
        /// </summary>
        private readonly IList<Task> _workerTasks = new List<Task>();

        /// <summary>
        /// A <see cref="CancellationTokenSource"/> used to notify Tasks that operations
        /// should be canceled.
        /// </summary>
        private readonly CancellationTokenSource _tokenSource = new CancellationTokenSource();

        /// <summary>
        /// Counter used to generate a unique id when registering a <see cref="ICallbackHandler"/>.
        /// </summary>
        private int _callbackCounter = 0;

        internal CallbackServer()
        {
            AppDomain.CurrentDomain.ProcessExit += (s, e) => Shutdown();

            s_logger.LogInfo($"Starting CallbackServer.");
            ISocketWrapper listener = SocketFactory.CreateSocket();
            Run(listener);
        }

        /// <summary>
        /// Produce a unique id and register a <see cref="ICallbackHandler"/> with it.
        /// </summary>
        /// <param name="callbackHandler">The handler to register.</param>
        /// <returns>A unique id associated with the handler.</returns>
        internal int RegisterCallback(ICallbackHandler callbackHandler)
        {
            int callbackId = Interlocked.Increment(ref _callbackCounter);
            _callbackHandlers[callbackId] = callbackHandler;

            return callbackId;
        }

        /// <summary>
        /// Runs the callback server.
        /// </summary>
        private void Run(ISocketWrapper listener)
        {
            try
            {
                listener.Listen();
                _tokenSource.Token.Register(() => listener.Dispose());

                // Communicate with the JVM the callback server's address and port.
                IPEndPoint localEndPoint = (IPEndPoint)listener.LocalEndPoint;
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    "DotnetHandler",
                    "connectCallback",
                    localEndPoint.Address.ToString(),
                    localEndPoint.Port);

                s_logger.LogInfo($"Started CallbackServer on {localEndPoint}");

                // Start accepting connections from JVM.
                Task.Factory.StartNew(
                    () => StartServer(listener),
                    _tokenSource.Token,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default);
            }
            catch (Exception e)
            {
                s_logger.LogError($"CallbackServer exiting with exception: {e}");
                Shutdown();
            }
        }

        /// <summary>
        /// Starts listening to any connection from JVM.
        /// </summary>
        /// <param name="listener"></param>
        private void StartServer(ISocketWrapper listener)
        {
            try
            {
                long connectionId = 1;

                while (true)
                {
                    ISocketWrapper socket = listener.Accept();
                    CallbackConnection connection =
                        new CallbackConnection(connectionId, socket, _callbackHandlers);

                    _waitingConnections.Add(connection);
                    _connections[connectionId] = connection;
                    ++connectionId;

                    int numConnections = _connections.Count;

                    // Start worker tasks until there are at least as many worker tasks
                    // as there are CallbackConnections. CallbackConnections are expected
                    // to stay open and reuse the socket to service repeated callback
                    // requests. However, if there is an issue with a connection, then
                    // CallbackConnection.Run will return, freeing up extra worker tasks
                    // to service any _waitingConnections.
                    //
                    // For example, 
                    // Assume there were 5 worker tasks, each servicing a CallbackConnection
                    // (5 total healthy connections). If 2 CallbackConnection sockets closed
                    // unexpectedly, then there would be 5 worker tasks and 3 healthy
                    // connections. If a new connection request arrived, then the
                    // CallbackConnection would be added to the _waitingConnections collection
                    // and no new worker tasks would be started (2 worker tasks are already
                    // waiting to take CallbackConnections from _waitingConnections).
                    while (_workerTasks.Count < numConnections)
                    {
                        _workerTasks.Add(Task.Run(() => RunWorkerTask(), _tokenSource.Token));
                    }

                    s_logger.LogInfo(
                        $"Pool snapshot: [NumTasks:{_workerTasks.Count}], " +
                        $"[NumConnections:{numConnections}]");
                }
            }
            catch (Exception e)
            {
                s_logger.LogError($"StartServer() exits with exception: {e}");
                Shutdown();
            }
        }

        /// <summary>
        /// <see cref="RunWorkerTask"/> is called for each worker task when it starts.
        /// <see cref="RunWorkerTask"/> doesn't return (except for the error cases), and
        /// keeps pulling from <see cref="_waitingConnections"/> and runs the retrieved
        /// <see cref="CallbackConnection"/>.
        /// </summary>
        private void RunWorkerTask()
        {
            try
            {
                while (true)
                {
                    _tokenSource.Token.ThrowIfCancellationRequested();

                    if (_waitingConnections.TryTake(
                        out CallbackConnection connection,
                        Timeout.Infinite))
                    {
                        // The connection will only return when the connection is closing
                        // (via CancellationToken) or there are error cases.
                        connection.Run(_tokenSource.Token);

                        // Assume the connection is in a bad state, and do not reuse it.
                        // Remove it from _connections list to prevent the server task from
                        // creating more tasks than needed.
                        _connections.TryRemove(connection.ConnectionId, out CallbackConnection _);
                    }
                }
            }
            catch (Exception e)
            {
                s_logger.LogError($"RunWorkerTask() exits with an exception: {e}");
                Shutdown();
            }
        }

        /// <summary>
        /// Shuts down the <see cref="CallbackServer"/> by canceling any running tasks
        /// and disposing of resources.
        /// </summary>
        private void Shutdown()
        {
            _tokenSource.Cancel();
            _waitingConnections.Dispose();
            _connections.Clear();
            _callbackHandlers.Clear();
            _workerTasks.Clear();

            SparkEnvironment.JvmBridge.CallStaticJavaMethod("DotnetHandler", "closeCallback");
        }
    }
}
