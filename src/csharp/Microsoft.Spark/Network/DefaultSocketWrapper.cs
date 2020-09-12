// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Microsoft.Spark.Services;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Network
{
    /// <summary>
    /// A simple wrapper of System.Net.Sockets.Socket class.
    /// </summary>
    internal sealed class DefaultSocketWrapper : ISocketWrapper
    {
        private readonly Socket _innerSocket;
        private Stream _inputStream;
        private Stream _outputStream;

        /// <summary>
        /// Default constructor that creates a new instance of DefaultSocket class which represents
        /// a traditional socket (System.Net.Socket.Socket).
        /// 
        /// This socket is bound to Loopback with port 0.
        /// </summary>
        public DefaultSocketWrapper() :
            this(new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
        {
            _innerSocket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        }

        /// <summary>
        /// Initializes a instance of DefaultSocket class using the specified
        /// System.Net.Socket.Socket object.
        /// </summary>
        /// <param name="socket">The existing socket</param>
        private DefaultSocketWrapper(Socket socket)
        {
            // Disable Nagle algorithm, which works to combine small packets together at the
            // expense of responsiveness; it's focused on reducing congestion on slow networks,
            // but all of our accesses are on localhost.
            socket.NoDelay = true;

            _innerSocket = socket;
        }

        /// <summary>
        /// Releases all resources used by the current instance of the DefaultSocket class.
        /// </summary>
        public void Dispose()
        {
            _outputStream?.Dispose();
            _inputStream?.Dispose();
            _innerSocket.Dispose();
        }

        /// <summary>
        /// Accepts a incoming connection request.
        /// </summary>
        /// <returns>A DefaultSocket instance used to send and receive data</returns>
        public ISocketWrapper Accept() => new DefaultSocketWrapper(_innerSocket.Accept());

        /// <summary>
        /// Establishes a connection to a remote host that is specified by an IP address and
        /// a port number.
        /// </summary>
        /// <param name="remoteaddr">The IP address of the remote host</param>
        /// <param name="port">The port number of the remote host</param>
        /// <param name="secret">Secret string to use for connection</param>
        public void Connect(IPAddress remoteaddr, int port, string secret)
        {
            _innerSocket.Connect(new IPEndPoint(remoteaddr, port));

            if (!string.IsNullOrWhiteSpace(secret))
            {
                using NetworkStream stream = CreateNetworkStream();
                if (!Authenticator.AuthenticateAsClient(stream, secret))
                {
                    throw new Exception($"Failed to authenticate for port: {port}.");
                }
            }
        }

        /// <summary>
        /// Returns the NetworkStream used to send and receive data.
        /// </summary>
        /// <remarks>
        /// GetStream returns a NetworkStream that you can use to send and receive data.
        /// You must close/dispose the NetworkStream by yourself. Closing DefaultSocketWrapper
        /// does not release the NetworkStream.
        /// </remarks>
        /// <returns>The underlying Stream instance that be used to send and receive data</returns>
        private NetworkStream CreateNetworkStream() => new NetworkStream(_innerSocket);

        /// <summary>
        /// Returns a stream used to receive data only.
        /// </summary>
        /// <returns>The underlying Stream instance that be used to receive data</returns>
        public Stream InputStream =>
            _inputStream ??= CreateStream(
                ConfigurationService.WorkerReadBufferSizeEnvVarName);

        /// <summary>
        /// Returns a stream used to send data only.
        /// </summary>
        /// <returns>The underlying Stream instance that be used to send data</returns>
        public Stream OutputStream =>
            _outputStream ??= CreateStream(
                ConfigurationService.WorkerWriteBufferSizeEnvVarName);

        private Stream CreateStream(string bufferSizeEnvVarName)
        {
            string envVar = Environment.GetEnvironmentVariable(bufferSizeEnvVarName);
            if (string.IsNullOrEmpty(envVar) ||
                !int.TryParse(envVar, out var writeBufferSize))
            {
                // The default buffer size is 64K, PythonRDD also use 64K as default buffer size.
                writeBufferSize = 64 * 1024;
            }

            Stream ns = CreateNetworkStream();
            return (writeBufferSize > 0) ? new BufferedStream(ns, writeBufferSize) : ns;
        }

        /// <summary>
        /// Starts listening for incoming connections requests
        /// </summary>
        /// <param name="backlog">The maximum length of the pending connections queue. </param>
        public void Listen(int backlog = 16) => _innerSocket.Listen(backlog);

        /// <summary>
        /// Returns the local endpoint.
        /// </summary>
        public EndPoint LocalEndPoint => _innerSocket.LocalEndPoint;

        /// <summary>
        /// Returns the remote endpoint.
        /// </summary>
        public EndPoint RemoteEndPoint => _innerSocket.RemoteEndPoint;
    }
}
