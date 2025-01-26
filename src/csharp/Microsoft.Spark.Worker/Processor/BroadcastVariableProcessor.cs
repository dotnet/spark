// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Net;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Worker.Processor
{
    internal sealed class BroadcastVariableProcessor
    {
        private readonly Version _version;
        internal BroadcastVariableProcessor(Version version)
        {
            _version = version;
        }

        /// <summary>
        /// Reads the given stream to construct a BroadcastVariables object.
        /// </summary>
        /// <param name="stream">The stream to read from</param>
        /// <returns>BroadcastVariables object</returns>
        internal BroadcastVariables Process(Stream stream)
        {
            var broadcastVars = new BroadcastVariables();
            ISocketWrapper socket = null;

            broadcastVars.DecryptionServerNeeded = SerDe.ReadBool(stream);
            broadcastVars.Count = Math.Max(SerDe.ReadInt32(stream), 0);

            if (broadcastVars.DecryptionServerNeeded)
            {
                broadcastVars.DecryptionServerPort = SerDe.ReadInt32(stream);
                broadcastVars.Secret = SerDe.ReadString(stream);
                if (broadcastVars.Count > 0)
                {
                    socket = SocketFactory.CreateSocket();
                    socket.Connect(
                        IPAddress.Loopback,
                        broadcastVars.DecryptionServerPort,
                        broadcastVars.Secret);
                }
            }

            for (int i = 0; i < broadcastVars.Count; ++i)
            {
                long bid = SerDe.ReadInt64(stream);
                if (bid >= 0)
                {
                    if (broadcastVars.DecryptionServerNeeded)
                    {
                        long readBid = SerDe.ReadInt64(socket.InputStream);
                        if (bid != readBid)
                        {
                            throw new Exception("The Broadcast Id received from the encryption " +
                                $"server {readBid} is different from the Broadcast Id received " +
                                $"from the payload {bid}.");
                        }

                        var value = BinarySerDe.Deserialize<object>(socket.InputStream);
                        BroadcastRegistry.Add(bid, value);
                    }
                    else
                    {
                        string path = SerDe.ReadString(stream);
                        using FileStream fStream =
                            File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);

                        var value = BinarySerDe.Deserialize<object>(fStream);
                        BroadcastRegistry.Add(bid, value);
                    }
                }
                else
                {
                    bid = -bid - 1;
                    BroadcastRegistry.Remove(bid);
                }
            }
            socket?.Dispose();
            return broadcastVars;
        }
    }
}
