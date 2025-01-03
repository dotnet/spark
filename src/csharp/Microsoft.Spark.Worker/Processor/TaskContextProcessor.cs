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
                (3, _) t when t.Minor < 3 => TaskContextProcessorV3_0_X.Process(stream),
                (3, _) => TaskContextProcessorV3_3_X.Process(stream),
                _ => throw new NotSupportedException($"Spark {_version} not supported.")
            };
        }

        private static TaskContext ReadTaskContext_2_x(Stream stream)
        => new()
        {
            IsBarrier = SerDe.ReadBool(stream),
            Port = SerDe.ReadInt32(stream),
            Secret = SerDe.ReadString(stream),

            StageId = SerDe.ReadInt32(stream),
            PartitionId = SerDe.ReadInt32(stream),
            AttemptNumber = SerDe.ReadInt32(stream),
            AttemptId = SerDe.ReadInt64(stream),
        };

        // Needed for 3.3.0+
        // https://issues.apache.org/jira/browse/SPARK-36173
        private static TaskContext ReadTaskContext_3_3(Stream stream)
        => new()
        {
            IsBarrier = SerDe.ReadBool(stream),
            Port = SerDe.ReadInt32(stream),
            Secret = SerDe.ReadString(stream),

            StageId = SerDe.ReadInt32(stream),
            PartitionId = SerDe.ReadInt32(stream),
            AttemptNumber = SerDe.ReadInt32(stream),
            AttemptId = SerDe.ReadInt64(stream),
            CPUs = SerDe.ReadInt32(stream)
        };

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
                TaskContext taskContext = ReadTaskContext_2_x(stream);
                ReadTaskContextProperties(stream, taskContext);

                return taskContext;
            }
        }

        private static class TaskContextProcessorV3_0_X
        {
            internal static TaskContext Process(Stream stream)
            {
                TaskContext taskContext = ReadTaskContext_2_x(stream);
                ReadTaskContextResources(stream);
                ReadTaskContextProperties(stream, taskContext);

                return taskContext;
            }
        }

        private static class TaskContextProcessorV3_3_X
        {
            internal static TaskContext Process(Stream stream)
            {
                TaskContext taskContext = ReadTaskContext_3_3(stream);
                ReadTaskContextResources(stream);
                ReadTaskContextProperties(stream, taskContext);

                return taskContext;
            }
        }
    }
}
