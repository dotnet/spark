// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Net;
using System.Threading;
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

        private readonly IJvmBridge _jvm;

        /// <summary>
        /// Keeps track of all <see cref="ICallbackHandler"/>s by its Id. This is accessed
        /// by the <see cref="CallbackServer"/> and the <see cref="CallbackConnection"/>
        /// running in the worker threads.
        /// </summary>
        private readonly ConcurrentDictionary<int, ICallbackHandler> _callbackHandlers =
            new ConcurrentDictionary<int, ICallbackHandler>();

        /// <summary>
        /// Keeps track of all <see cref="CallbackConnection"/> objects identified by its
        /// <see cref="CallbackConnection.ConnectionId"/>. The main thread creates a
        /// <see cref="CallbackConnection"/> each time it receives a new socket connection
        /// from the JVM side and inserts it into <see cref="_connections"/>. Each worker
        /// thread calls <see cref="CallbackConnection.Run"/> and removes the connection
        /// once this call is finished. <see cref="CallbackConnection.Run"/> will not return
        /// unless the <see cref="CallbackConnection"/> needs to be closed.
        /// Also, <see cref="_connections"/> is used to bound the number of worker threads
        /// since it gives you the total number of active <see cref="CallbackConnection"/>s.
        /// </summary>
        private readonly ConcurrentDictionary<long, CallbackConnection> _connections =
            new ConcurrentDictionary<long, CallbackConnection>();

        /// <summary>
        /// Each worker thread picks up a CallbackConnection from _waitingConnections
        /// and runs it.
        /// </summary>
        private readonly BlockingCollection<CallbackConnection> _waitingConnections =
            new BlockingCollection<CallbackConnection>();

        /// <summary>
        /// A <see cref="CancellationTokenSource"/> used to notify threads that operations
        /// should be canceled.
        /// </summary>
        private readonly CancellationTokenSource _tokenSource = new CancellationTokenSource();

        /// <summary>
        /// Counter used to generate a unique id when registering a <see cref="ICallbackHandler"/>.
        /// </summary>
        private int _callbackCounter = 0;

        private bool _isRunning = false;

        private ISocketWrapper _listener;
        
        private JvmObjectReference _jvmCallbackClient;

        internal int CurrentNumConnections => _connections.Count;

        internal JvmObjectReference JvmCallbackClient
        {
            get
            {
                if (_jvmCallbackClient is null)
                {
                    throw new InvalidOperationException(
                        "Please make sure that CallbackServer was started before accessing JvmCallbackClient.");
                }

                return _jvmCallbackClient;
            }
        }

        internal CallbackServer(IJvmBridge jvm, bool run = true)
        {
            AppDomain.CurrentDomain.ProcessExit += (s, e) => Shutdown();
            _jvm = jvm;

            if (run)
            {
                Run();
            }
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
        /// <param name="listener">The listening socket.</param>
        internal void Run(ISocketWrapper listener)
        {
            if (_isRunning)
            {
                s_logger.LogWarn("CallbackServer is already running.");
                return;
            }

            s_logger.LogInfo($"Starting CallbackServer.");
            _isRunning = true;

            try
            {
                _listener = listener;
                _listener.Listen();

                // Communicate with the JVM the callback server's address and port.
                var localEndPoint = (IPEndPoint)_listener.LocalEndPoint;
                _jvmCallbackClient = (JvmObjectReference)_jvm.CallStaticJavaMethod(
                    "DotnetHandler",
                    "connectCallback",
                    localEndPoint.Address.ToString(),
                    localEndPoint.Port);

                s_logger.LogInfo($"Started CallbackServer on {localEndPoint}");

                // Start accepting connections from JVM.
                new Thread(() => StartServer(_listener))
                {
                    IsBackground = true
                }.Start();
            }
            catch (Exception e)
            {
                s_logger.LogError($"CallbackServer exiting with exception: {e}");
                Shutdown();
            }
        }

        /// <summary>
        /// Runs the callback server.
        /// </summary>
        private void Run()
        {
            Run(SocketFactory.CreateSocket());
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
                int numWorkerThreads = 0;

                while (_isRunning)
                {
                    ISocketWrapper socket = listener.Accept();
                    var connection =
                        new CallbackConnection(connectionId, socket, _callbackHandlers);

                    _waitingConnections.Add(connection);
                    _connections[connectionId] = connection;
                    ++connectionId;

                    int numConnections = CurrentNumConnections;

                    // Start worker thread until there are at least as many worker threads
                    // as there are CallbackConnections. CallbackConnections are expected
                    // to stay open and reuse the socket to service repeated callback
                    // requests. However, if there is an issue with a connection, then
                    // CallbackConnection.Run will return, freeing up extra worker threads
                    // to service any _waitingConnections.
                    //
                    // For example, 
                    // Assume there were 5 worker threads, each servicing a CallbackConnection
                    // (5 total healthy connections). If 2 CallbackConnection sockets closed
                    // unexpectedly, then there would be 5 worker threads and 3 healthy
                    // connections. If a new connection request arrived, then the
                    // CallbackConnection would be added to the _waitingConnections collection
                    // and no new worker threads would be started (2 worker threads are already
                    // waiting to take CallbackConnections from _waitingConnections).
                    while (numWorkerThreads < numConnections)
                    {
                        new Thread(RunWorkerThread)
                        {
                            IsBackground = true
                        }.Start();
                        ++numWorkerThreads;
                    }

                    s_logger.LogInfo(
                        $"Pool snapshot: [NumThreads:{numWorkerThreads}], " +
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
        /// <see cref="RunWorkerThread"/> is called for each worker thread when it starts.
        /// <see cref="RunWorkerThread"/> doesn't return (except for the error cases), and
        /// keeps pulling from <see cref="_waitingConnections"/> and runs the retrieved
        /// <see cref="CallbackConnection"/>.
        /// </summary>
        private void RunWorkerThread()
        {
            try
            {
                while (_isRunning)
                {
                    if (_waitingConnections.TryTake(
                        out CallbackConnection connection,
                        Timeout.Infinite))
                    {
                        // The connection will only return when the connection is closing
                        // (via CancellationToken) or there are error cases.
                        connection.Run(_tokenSource.Token);

                        // Assume the connection is in a bad state, and do not reuse it.
                        // Remove it from _connections list to prevent the server thread from
                        // creating more threads than needed.
                        _connections.TryRemove(connection.ConnectionId, out CallbackConnection _);
                    }
                }
            }
            catch (Exception e)
            {
                s_logger.LogError($"RunWorkerThread() exits with an exception: {e}");
                Shutdown();
            }
        }

        /// <summary>
        /// Shuts down the <see cref="CallbackServer"/> by canceling any running threads
        /// and disposing of resources.
        /// </summary>
        private void Shutdown()
        {
            s_logger.LogInfo("Shutting down CallbackServer");

            _tokenSource.Cancel();
            _waitingConnections.Dispose();
            _connections.Clear();
            _callbackHandlers.Clear();
            _listener?.Dispose();
            _isRunning = false;

            _jvm.CallStaticJavaMethod("DotnetHandler", "closeCallback");
        }
    }
}
