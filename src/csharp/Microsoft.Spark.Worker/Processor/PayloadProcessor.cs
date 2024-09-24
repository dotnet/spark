// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Services;
using Microsoft.Spark.Worker.Utils;

namespace Microsoft.Spark.Worker.Processor
{
    /// <summary>
    /// PayloadProcessor reads the stream and constructs a Payload object.
    /// </summary>
    internal class PayloadProcessor
    {
        private readonly Version _version;

        internal PayloadProcessor(Version version)
        {
            _version = version;
        }

        /// <summary>
        /// Processes the given stream to construct a Payload object.
        /// </summary>
        /// <param name="stream">The stream to read from</param>
        /// <returns>
        /// Returns a valid payload object if the stream contains all the necessary data.
        /// Returns null if the stream is already closed at the beginning of the read.
        /// </returns>
        internal Payload Process(Stream stream)
        {
            var payload = new Payload();

            byte[] splitIndexBytes;
            try
            {
                splitIndexBytes = SerDe.ReadBytes(stream, sizeof(int));
                // For socket stream, read on the stream returns 0, which
                // SerDe.ReadBytes() returns as null to denote the stream is closed.
                if (splitIndexBytes == null)
                {
                    return null;
                }
            }
            catch (ObjectDisposedException)
            {
                // For stream implementation such as MemoryStream will throw
                // ObjectDisposedException if the stream is already closed.
                return null;
            }

            payload.SplitIndex = BinaryPrimitives.ReadInt32BigEndian(splitIndexBytes);
            payload.Version = SerDe.ReadString(stream);

            payload.TaskContext = new TaskContextProcessor(_version).Process(stream);
            TaskContextHolder.Set(payload.TaskContext);

            payload.SparkFilesDir = SerDe.ReadString(stream);
            SparkFiles.SetRootDirectory(payload.SparkFilesDir);

            // Register additional assembly handlers after SparkFilesDir has been set
            // and before any deserialization occurs. BroadcastVariableProcessor may
            // deserialize objects from assemblies that are not currently loaded within
            // our current context.
            AssemblyLoaderHelper.RegisterAssemblyHandler();

            if (ConfigurationService.IsDatabricks)
            {
                SerDe.ReadString(stream);
                SerDe.ReadString(stream);
            }

            payload.IncludeItems = ReadIncludeItems(stream);
            payload.BroadcastVariables = new BroadcastVariableProcessor(_version).Process(stream);

            // TODO: Accumulate registration should be done here.

            payload.Command = new CommandProcessor(_version).Process(stream);

            return payload;
        }

        /// <summary>
        /// Reads the given stream to construct a string array of the include items.
        /// </summary>
        /// <param name="stream">The stream to read from</param>
        /// <returns>Array of include items</returns>
        private static IEnumerable<string> ReadIncludeItems(Stream stream)
        {
            int numIncludeItems = Math.Max(SerDe.ReadInt32(stream), 0);

            var includeItems = new string[numIncludeItems];
            for (int i = 0; i < numIncludeItems; ++i)
            {
                includeItems[i] = SerDe.ReadString(stream);
            }

            return includeItems;
        }
    }
}
