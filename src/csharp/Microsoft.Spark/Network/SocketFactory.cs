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
            return new DefaultSocketWrapper();
        }
    }
}
