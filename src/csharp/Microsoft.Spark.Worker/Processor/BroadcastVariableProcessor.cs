// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop.Ipc;

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

            // Note that broadcast variables are currently ignored.
            // Thus, just read the info from stream without handling them.
            int numBroadcastVariables = Math.Max(SerDe.ReadInt32(stream), 0);
            if (broadcastVars.DecryptionServerNeeded)
            {
                broadcastVars.DecryptionServerPort = SerDe.ReadInt32(stream);
                broadcastVars.Secret = SerDe.ReadString(stream);

                // TODO: Handle the authentication.
            }

            for (int i = 0; i < numBroadcastVariables; ++i)
            {
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
                        // TODO: Register new broadcast variable.
                    }
                }
                else
                {
                    bid = -bid - 1;
                    // TODO: Remove registered broadcast variable.
                }
            }

            return broadcastVars;
        }
    }
}
