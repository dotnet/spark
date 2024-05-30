// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Services;
using Newtonsoft.Json.Linq;

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
            } ;
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
            Console.Error.WriteLine("coming here new1");
            // Read barrier-related payload. Note that barrier is currently not supported.
            SerDe.ReadBool(stream); // IsBarrier
            SerDe.ReadInt32(stream); // BoundPort
            SerDe.ReadString(stream); // Secret
            Console.Error.WriteLine("coming here new2");
        }

        private static void ReadTaskContextProperties(Stream stream, TaskContext taskContext)
        {
            Console.Error.WriteLine("coming here new5");
            int numProperties = SerDe.ReadInt32(stream);
            Console.Error.WriteLine("coming here new5.1 numProperties: " + numProperties.ToString());
            for (int i = 0; i < numProperties; ++i)
            {
                Console.Error.WriteLine("coming here new5 i: " + i.ToString());
                string key = SerDe.ReadString(stream);
                string value = SerDe.ReadString(stream);
                Console.Error.WriteLine("coming here new5 key: " + key + " value: " + value);
                taskContext.LocalProperties.Add(key, value);
            }
            Console.Error.WriteLine("coming here new6");
        }

        private static void ReadTaskContextResources(Stream stream)
        {
            Console.Error.WriteLine("coming here new3");
            // Currently, resources are not supported.
            int numResources = SerDe.ReadInt32(stream);
            Console.Error.WriteLine("coming here new3.1 numResources: " + numResources.ToString());
            for (int i = 0; i < numResources; ++i)
            {
                Console.Error.WriteLine("coming here new3.2 i: " + i.ToString());
                string key = SerDe.ReadString(stream); // key
                string value = SerDe.ReadString(stream); // value
                int numAddresses = SerDe.ReadInt32(stream);
                Console.Error.WriteLine("coming here new3.3 numAddresses: " + numAddresses + " " + key +  " " + value);
                for (int j = 0; j < numAddresses; ++j)
                {
                    Console.Error.WriteLine("coming here new3.4 j: " + j.ToString());
                    string s =SerDe.ReadString(stream); // address
                    Console.Error.WriteLine("stream" + s);
                }
            }
            Console.Error.WriteLine("coming here new4");
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
            private static readonly ILoggerService s_logger =
               LoggerServiceFactory.GetLogger(typeof(TaskContextProcessorV3_0_X));
            internal static TaskContext Process(Stream stream)
            {
                s_logger.LogInfo($"Coming here inside TaskContextProcessorV3_0_X");
                ReadBarrierInfo(stream);
                TaskContext taskContext = ReadTaskContext(stream);
                Console.Error.WriteLine("coming here new2");
                ReadTaskContextResources(stream);
                ReadTaskContextProperties(stream, taskContext);
                Console.Error.WriteLine("coming here new7");
                return taskContext;
            }
        }
    }
}
