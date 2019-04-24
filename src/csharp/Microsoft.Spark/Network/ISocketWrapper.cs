// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Net;

namespace Microsoft.Spark.Network
{
    /// <summary>
    /// ISocketWrapper interface defines the common methods to operate a socket.
    /// </summary>
    internal interface ISocketWrapper : IDisposable
    {
        /// <summary>
        /// Accepts a incoming connection request.
        /// </summary>
        /// <returns>A ISocket instance used to send and receive data</returns>
        ISocketWrapper Accept();

        /// <summary>
        /// Establishes a connection to a remote host that is specified by an IP address and
        /// a port number.
        /// </summary>
        /// <param name="remoteaddr">The IP address of the remote host</param>
        /// <param name="port">The port number of the remote host</param>
        /// <param name="secret">Optional secret string to use for connection</param>
        void Connect(IPAddress remoteaddr, int port, string secret = null);

        /// <summary>
        /// Returns a stream used to receive data only.
        /// </summary>
        /// <returns>The underlying Stream instance that be used to receive data</returns>
        Stream InputStream { get; }

        /// <summary>
        /// Returns a stream used to send data only.
        /// </summary>
        /// <returns>The underlying Stream instance that be used to send data</returns>
        Stream OutputStream { get; }

        /// <summary>
        /// Starts listening for incoming connections requests
        /// </summary>
        /// <param name="backlog">The maximum length of the pending connections queue</param>
        void Listen(int backlog = 16);

        /// <summary>
        /// Returns the local endpoint.
        /// </summary>
        EndPoint LocalEndPoint { get; }
    }
}
