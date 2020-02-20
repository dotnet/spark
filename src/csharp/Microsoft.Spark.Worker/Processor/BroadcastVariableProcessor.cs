// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;

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

            if (_version >= new Version(Versions.V2_3_2))
            {
                broadcastVars.DecryptionServerNeeded = SerDe.ReadBool(stream);
            }

            int numBroadcastVariables = Math.Max(SerDe.ReadInt32(stream), 0);
            broadcastVars.Count = numBroadcastVariables;

            if (broadcastVars.DecryptionServerNeeded)
            {
                broadcastVars.DecryptionServerPort = SerDe.ReadInt32(stream);
                broadcastVars.Secret = SerDe.ReadString(stream);
            }

            for (int i = 0; i < numBroadcastVariables; ++i)
            {
                var formatter = new BinaryFormatter();
                long bid = SerDe.ReadInt64(stream);
                if (bid >= 0)
                {
                    if (broadcastVars.DecryptionServerNeeded)
                    {
                        using ISocketWrapper socket = SocketFactory.CreateSocket();
                        socket.Connect(
                            IPAddress.Loopback,
                            broadcastVars.DecryptionServerPort,
                            broadcastVars.Secret);
                        long readBid = SerDe.ReadInt64(socket.InputStream);
                        if (bid == readBid)
                        {
                            var value = formatter.Deserialize(socket.InputStream);
                            BroadcastRegistry._registry.TryAdd(bid, value);
                        }
                    }
                    else
                    {
                        var path = SerDe.ReadString(stream);
                        FileStream fStream = File.Open(path, FileMode.Open, FileAccess.Read);
                        var value = formatter.Deserialize(fStream);
                        fStream.Close();
                        BroadcastRegistry._registry.TryAdd(bid, value);
                    }
                }
                else
                {
                    bid = -bid - 1;
                    BroadcastRegistry._registry.TryRemove(bid, out _);
                    BroadcastRegistry.listBroadcastVariables.Remove((int)bid);
                }
            }
            return broadcastVars;
        }
    }
}
