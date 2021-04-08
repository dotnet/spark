// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Worker.Processor
{
    internal sealed class TaskContextProcessor
    {
        private readonly Version _version;

        internal TaskContextProcessor(Version version)
        {
            _version = version;
        }

        internal TaskContext Process(Stream stream)
        {
            return (_version.Major, _version.Minor) switch
            {
                (2, 4) => TaskContextProcessorV2_4_X.Process(stream),
                (3, _) => TaskContextProcessorV3_0_X.Process(stream),
                _ => throw new NotSupportedException($"Spark {_version} not supported.")
            };
        }

        private static TaskContext ReadTaskContext(Stream stream)
        {
            return new TaskContext
            {
                StageId = SerDe.ReadInt32(stream),
                PartitionId = SerDe.ReadInt32(stream),
                AttemptNumber = SerDe.ReadInt32(stream),
                AttemptId = SerDe.ReadInt64(stream)
            };
        }

        private static void ReadBarrierInfo(Stream stream)
        {
            // Read barrier-related payload. Note that barrier is currently not supported.
            SerDe.ReadBool(stream); // IsBarrier
            SerDe.ReadInt32(stream); // BoundPort
            SerDe.ReadString(stream); // Secret
        }

        private static void ReadTaskContextProperties(Stream stream, TaskContext taskContext)
        {
            int numProperties = SerDe.ReadInt32(stream);
            for (int i = 0; i < numProperties; ++i)
            {
                string key = SerDe.ReadString(stream);
                string value = SerDe.ReadString(stream);
                taskContext.LocalProperties.Add(key, value);
            }
        }

        private static void ReadTaskContextResources(Stream stream)
        {
            // Currently, resources are not supported.
            int numResources = SerDe.ReadInt32(stream);
            for (int i = 0; i < numResources; ++i)
            {
                SerDe.ReadString(stream); // key
                SerDe.ReadString(stream); // value
                int numAddresses = SerDe.ReadInt32(stream);
                for (int j = 0; j < numAddresses; ++j)
                {
                    SerDe.ReadString(stream); // address
                }
            }
        }

        private static class TaskContextProcessorV2_4_X
        {
            internal static TaskContext Process(Stream stream)
            {
                ReadBarrierInfo(stream);
                TaskContext taskContext = ReadTaskContext(stream);
                ReadTaskContextProperties(stream, taskContext);

                return taskContext;
            }
        }

        private static class TaskContextProcessorV3_0_X
        {
            internal static TaskContext Process(Stream stream)
            {
                ReadBarrierInfo(stream);
                TaskContext taskContext = ReadTaskContext(stream);
                ReadTaskContextResources(stream);
                ReadTaskContextProperties(stream, taskContext);

                return taskContext;
            }
        }
    }
}
