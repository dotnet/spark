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
            if (_version.Major == 2)
            {
                switch (_version.Minor)
                {
                    case 3:
                        return TaskContextProcessorV2_3_X.Process(stream);
                    case 4:
                        return TaskContextProcessorV2_4_X.Process(stream);
                }
            }

            throw new NotSupportedException($"Spark {_version} not supported.");
        }

        private static class TaskContextProcessorV2_3_X
        {
            internal static TaskContext Process(Stream stream)
            {
                return new TaskContext
                {
                    StageId = SerDe.ReadInt32(stream),
                    PartitionId = SerDe.ReadInt32(stream),
                    AttemptNumber = SerDe.ReadInt32(stream),
                    AttemptId = SerDe.ReadInt64(stream)
                };
            }
        }

        private static class TaskContextProcessorV2_4_X
        {
            internal static TaskContext Process(Stream stream)
            {
                // Read barrier-related payload. Note that barrier is currently not supported.
                SerDe.ReadBool(stream); // IsBarrier
                SerDe.ReadInt32(stream); // BoundPort
                SerDe.ReadString(stream); // Secret

                var taskContext = new TaskContext
                {
                    StageId = SerDe.ReadInt32(stream),
                    PartitionId = SerDe.ReadInt32(stream),
                    AttemptNumber = SerDe.ReadInt32(stream),
                    AttemptId = SerDe.ReadInt64(stream)
                };

                int numProperties = SerDe.ReadInt32(stream);
                for (int i = 0; i < numProperties; ++i)
                {
                    string key = SerDe.ReadString(stream);
                    string value = SerDe.ReadString(stream);
                    taskContext.LocalProperties.Add(key, value);
                }

                return taskContext;
            }
        }
    }
}
