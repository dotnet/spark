// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Net;
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

                        // long readBid = SerDe.ReadInt64(stream);
                        // Console.WriteLine($"readBid: {readBid}");

                    }
                    else
                    {
                        var path = SerDe.ReadString(stream);
                        FileStream fStream = File.Open(path, FileMode.Open, FileAccess.Read);
                        object value = formatter.Deserialize(fStream);
                        fStream.Close();
                        BroadcastRegistry._registry.TryAdd(bid, value);
                    }
                }
                else
                {
                    bid = -bid - 1;
                    object value;
                    BroadcastRegistry._registry.TryRemove(bid, out value);
                    BroadcastRegistry.listBroadcastVariables.Remove((int)bid);
                }
            }
            if (broadcastVars.DecryptionServerNeeded)
            {
                using ISocketWrapper socket2 = SocketFactory.CreateSocket();
                socket2.Connect(
                    IPAddress.Loopback,
                    broadcastVars.DecryptionServerPort,
                    broadcastVars.Secret);
                var bid = SerDe.ReadInt64(socket2.InputStream);
                byte[] buffer = new byte[2];
                SerDe.TryReadBytes(socket2.InputStream, buffer, 1);
                //var bid2 = SerDe.ReadInt64(socket2.InputStream);
                var formatter = new BinaryFormatter();
                var value = formatter.Deserialize(socket2.InputStream);

            }

            Console.WriteLine("done processing broadcast vars");
            return broadcastVars;
        }
    }
}
