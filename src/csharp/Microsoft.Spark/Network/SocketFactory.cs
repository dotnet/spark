// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.Network
{
    /// <summary>
    /// SocketFactory is used to create ISocketWrapper instance.
    /// </summary>
    internal static class SocketFactory
    {
        /// <summary>
        /// Creates an ISocket instance based on the socket type set.
        /// </summary>
        /// <returns>
        /// ISocketWrapper instance.
        /// </returns>
        public static ISocketWrapper CreateSocket()
        {
            return CreateSocket(useBufferedStreams: true);
        }

        /// <summary>
        /// Creates a socket with optional stream buffering.
        /// </summary>
        /// <param name="useBufferedStreams">Whether to apply configured stream buffers</param>
        /// <returns>ISocketWrapper instance.</returns>
        public static ISocketWrapper CreateSocket(bool useBufferedStreams)
        {
            return new DefaultSocketWrapper(useBufferedStreams);
        }
    }
}
