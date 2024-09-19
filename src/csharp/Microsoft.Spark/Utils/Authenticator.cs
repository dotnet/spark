// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// Authenticator provides functionalities to authenticate between
    /// Spark and .NET worker.
    /// </summary>
    internal static class Authenticator
    {
        private static readonly string s_validResponseCode = "ok";
        private static readonly string s_invalidResponseCode = "err";

        /// <summary>
        /// Authenticates by writing secret to stream and validate the response.
        /// </summary>
        /// <param name="stream">Valid stream.</param>
        /// <param name="secret">Secret string to authenticate against.</param>
        /// <returns>True if authentication succeeds.</returns>
        public static bool AuthenticateAsClient(Stream stream, string secret)
        {
            SerDe.Write(stream, secret);
            stream.Flush();

            return SerDe.ReadString(stream) == s_validResponseCode;
        }

        /// <summary>
        /// Authenticates by reading secret from stream and writes the response code
        /// back to the stream.
        /// </summary>
        /// <param name="socket">Valid socket.</param>
        /// <param name="secret">Secret string to authenticate against.</param>
        /// <returns>True if authentication succeeds.</returns>
        public static bool AuthenticateAsServer(ISocketWrapper socket, string secret)
        {
            string clientSecret = SerDe.ReadString(socket.InputStream);

            bool result;
            if (clientSecret == secret)
            {
                SerDe.Write(socket.OutputStream, s_validResponseCode);
                result = true;
            }
            else
            {
                SerDe.Write(socket.OutputStream, s_invalidResponseCode);
                result = false;
            }

            socket.OutputStream.Flush();
            return result;
        }
    }
}
