// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;
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

            if (_version >= new Version(Versions.V2_3_2))
            {
                broadcastVars.DecryptionServerNeeded = SerDe.ReadBool(stream);
            }

            broadcastVars.Count = Math.Max(SerDe.ReadInt32(stream), 0);

            if (broadcastVars.DecryptionServerNeeded)
            {
                broadcastVars.DecryptionServerPort = SerDe.ReadInt32(stream);
                broadcastVars.Secret = SerDe.ReadString(stream);
                // TODO: Handle the authentication.
            }

            for (int i = 0; i < broadcastVars.Count; ++i)
            {
                var formatter = new BinaryFormatter();
                long bid = SerDe.ReadInt64(stream);
                if (bid >= 0)
                {
                    if (broadcastVars.DecryptionServerNeeded)
                    {
                        throw new NotImplementedException(
                            "broadcastDecryptionServer is not implemented.");
                    }
                    else
                    {
                        string path = SerDe.ReadString(stream);
                        using FileStream fStream = File.Open(path, FileMode.Open, FileAccess.Read);
                        formatter.Binder = new DeserializationBinder();
                        var value = formatter.Deserialize(fStream);
                        BroadcastRegistry.s_registry.TryAdd(bid, value);
                    }
                }
                else
                {
                    bid = -bid - 1;
                    BroadcastRegistry.s_registry.TryRemove(bid, out _);
                    BroadcastRegistry.s_listBroadcastVariables.Remove((int)bid);
                }
            }
            return broadcastVars;
        }
    }

    /// <summary>
    /// Function that loads the application assembly and returns the Type of the broadcast
    /// variable.
    /// </summary>
    sealed class DeserializationBinder : SerializationBinder
    {
        public override Type BindToType(string assemblyName, string typeName)
        {
            Type broadcastType = AssemblyLoader.ResolveAssembly(assemblyName).GetType(typeName);
            return broadcastType;
        }
    }
}
