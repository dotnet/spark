// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using Microsoft.Spark.Network;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// CallbackConnection is used to process the callback communication between
    /// Dotnet and the JVM. It uses a TCP socket to communicate with the JVM side
    /// and the socket is expected to be reused.
    /// </summary>
    internal sealed class CallbackConnection
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(CallbackConnection));

        private readonly ISocketWrapper _socket;

        /// <summary>
        /// Keeps track of all <see cref="ICallbackHandler"/>s by its Id. This is accessed
        /// by the <see cref="CallbackServer"/> and the <see cref="CallbackConnection"/>.
        /// </summary>
        private readonly ConcurrentDictionary<int, ICallbackHandler> _callbackHandlers;

        private volatile bool _isRunning = false;

        private int _numCallbacksRun = 0;

        internal CallbackConnection(
            long connectionId,
            ISocketWrapper socket,
            ConcurrentDictionary<int, ICallbackHandler> callbackHandlers)
        {
            ConnectionId = connectionId;
            _socket = socket;
            _callbackHandlers = callbackHandlers;

            s_logger.LogInfo(
                $"[{ConnectionId}] Connected with RemoteEndPoint: {socket.RemoteEndPoint}");
        }

        internal enum ConnectionStatus
        {
            /// <summary>
            /// Connection is normal.
            /// </summary>
            OK,

            /// <summary>
            /// Socket is closed by the JVM.
            /// </summary>
            SOCKET_CLOSED,

            /// <summary>
            /// Request to close connection.
            /// </summary>
            REQUEST_CLOSE
        }

        internal long ConnectionId { get; }

        internal bool IsRunning => _isRunning;

        /// <summary>
        /// Run and start processing the callback connection.
        /// </summary>
        /// <param name="token">Cancellation token used to stop the connection.</param>
        internal void Run(CancellationToken token)
        {
            _isRunning = true;
            Stream inputStream = _socket.InputStream;
            Stream outputStream = _socket.OutputStream;

            token.Register(() => Stop());

            try
            {
                while (_isRunning)
                {
                    ConnectionStatus connectionStatus =
                        ProcessStream(inputStream, outputStream, out bool readComplete);

                    if (connectionStatus == ConnectionStatus.OK)
                    {
                        outputStream.Flush();

                        ++_numCallbacksRun;

                        // If the socket is not read through completely, then it cannot be reused.
                        if (!readComplete)
                        {
                            _isRunning = false;

                            // Wait for server to complete to avoid 'connection reset' exception.
                            s_logger.LogInfo(
                                $"[{ConnectionId}] Sleep 500 millisecond to close socket.");
                            Thread.Sleep(500);
                        }
                    }
                    else if (connectionStatus == ConnectionStatus.REQUEST_CLOSE)
                    {
                        _isRunning = false;
                        s_logger.LogInfo(
                            $"[{ConnectionId}] Request to close connection received.");
                    }
                    else
                    {
                        _isRunning = false;
                        s_logger.LogWarn($"[{ConnectionId}] Socket is closed by JVM.");
                    }
                }
            }
            catch (Exception e)
            {
                _isRunning = false;
                s_logger.LogError($"[{ConnectionId}] Exiting with exception: {e}");
            }
            finally
            {
                try
                {
                    _socket.Dispose();
                }
                catch (Exception e)
                {
                    s_logger.LogWarn($"[{ConnectionId}] Exception while closing socket {e}");
                }

                s_logger.LogInfo(
                    $"[{ConnectionId}] Finished running {_numCallbacksRun} callback(s).");
            }
        }

        private void Stop()
        {
            _isRunning = false;
            s_logger.LogInfo($"[{ConnectionId}] Stopping CallbackConnection.");
        }

        /// <summary>
        /// Process the input and output streams.
        /// </summary>
        /// <param name="inputStream">The input stream.</param>
        /// <param name="outputStream">The output stream.</param>
        /// <param name="readComplete">True if stream is read completely, false otherwise.</param>
        /// <returns>The connection status.</returns>
        private ConnectionStatus ProcessStream(
            Stream inputStream,
            Stream outputStream,
            out bool readComplete)
        {
            readComplete = false;

            try
            {
                byte[] requestFlagBytes = SerDe.ReadBytes(inputStream, sizeof(int));
                // For socket stream, read on the stream returns 0, which
                // SerDe.ReadBytes() returns as null to denote the stream is closed.
                if (requestFlagBytes == null)
                {
                    return ConnectionStatus.SOCKET_CLOSED;
                }

                // Check value of the initial request. Expected values are:
                // - CallbackFlags.CLOSE
                // - CallbackFlags.CALLBACK
                int requestFlag = BinaryPrimitives.ReadInt32BigEndian(requestFlagBytes);
                if (requestFlag == (int)CallbackFlags.CLOSE) {
                    return ConnectionStatus.REQUEST_CLOSE;
                }
                else if (requestFlag != (int)CallbackFlags.CALLBACK)
                {
                    throw new Exception(
                        string.Format(
                            "Unexpected callback flag received. Expected: {0}, Received: {1}.",
                            CallbackFlags.CALLBACK,
                            requestFlag));
                }

                // Use callback id to get the registered handler.
                int callbackId = SerDe.ReadInt32(inputStream);
                if (!_callbackHandlers.TryGetValue(
                        callbackId,
                        out ICallbackHandler callbackHandler))
                {
                    throw new Exception($"Unregistered callback id: {callbackId}");
                }

                s_logger.LogInfo(
                    string.Format(
                        "[{0}] Received request for callback id: {1}, callback handler: {2}",
                        ConnectionId,
                        callbackId,
                        callbackHandler));

                // Save contents of callback handler data to be used later.
                using var callbackDataStream =
                    new MemoryStream(SerDe.ReadBytes(inputStream, SerDe.ReadInt32(inputStream)));

                // Check the end of stream.
                int endOfStream = SerDe.ReadInt32(inputStream);
                if (endOfStream == (int)CallbackFlags.END_OF_STREAM)
                {
                    s_logger.LogDebug($"[{ConnectionId}] Received END_OF_STREAM signal.");

                    // Run callback handler.
                    callbackHandler.Run(callbackDataStream);

                    SerDe.Write(outputStream, (int)CallbackFlags.END_OF_STREAM);
                    readComplete = true;
                }
                else
                {
                    // This may happen when the input data is not read completely.
                    s_logger.LogWarn(
                        $"[{ConnectionId}] Unexpected end of stream: {endOfStream}.");

                    // Write flag to indicate the connection should be closed.
                    SerDe.Write(outputStream, (int)CallbackFlags.CLOSE);
                }

                return ConnectionStatus.OK;
            }
            catch (Exception e)
            {
                s_logger.LogError($"[{ConnectionId}] ProcessStream() failed with exception: {e}");

                try
                {
                    SerDe.Write(outputStream, (int)CallbackFlags.DOTNET_EXCEPTION_THROWN);
                    SerDe.Write(outputStream, e.ToString());
                }
                catch (IOException)
                {
                    // JVM closed the socket.
                }
                catch (Exception ex)
                {
                    s_logger.LogError(
                        $"[{ConnectionId}] Writing exception to stream failed with exception: {ex}");
                }

                throw;
            }
        }
    }

    /// <summary>
    /// Enums with which the Dotnet CallbackConnection communicates with
    /// the JVM CallbackConnection.
    /// </summary>
    internal enum CallbackFlags : int
    {
        /// <summary>
        /// Flag to indicate connection should be closed.
        /// </summary>
        CLOSE = -1,

        /// <summary>
        /// Flag to indiciate callback should be called.
        /// </summary>
        CALLBACK = -2,

        /// <summary>
        /// Flag to indicate an exception thrown from dotnet.
        /// </summary>
        DOTNET_EXCEPTION_THROWN = -3,

        /// <summary>
        /// Flag to indicate end of stream.
        /// </summary>
        END_OF_STREAM = -4
    }
}
