// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Worker.UnitTest
{
    /// <summary>
    /// TestData provides helper functions to create default test data.
    /// </summary>
    internal static class TestData
    {
        public static IEnumerable<object[]> VersionData() =>
            new List<object[]>
            {
                new object[] { Versions.V2_4_0 },
                new object[] { Versions.V3_0_0 },
                new object[] { Versions.V3_2_0 },
                new object[] { Versions.V3_3_0 },
                new object[] { Versions.V3_5_1 },
            };

        internal static Payload GetDefaultPayload()
        {
            var taskContext = new TaskContext()
            {
                StageId = 1,
                PartitionId = 2,
                AttemptNumber = 1,
                AttemptId = 100L,
                Port = 9999,
                Secret = "secret"
            };

            var broadcastVars = new BroadcastVariables()
            {
                DecryptionServerNeeded = true,
                DecryptionServerPort = 9999,
                Secret = "secret"
            };

            return new Payload()
            {
                SplitIndex = 10,
                Version = AssemblyInfoProvider.MicrosoftSparkAssemblyInfo().AssemblyVersion,
                TaskContext = taskContext,
                SparkFilesDir = "directory",
                IncludeItems = new[] { "file1", "file2" },
                BroadcastVariables = broadcastVars
            };
        }

        internal static CommandPayload GetDefaultCommandPayload()
        {
            var udfWrapper1 = new PicklingUdfWrapper<string, string>((str) => $"udf1 {str}");
            var udfWrapper2 = new PicklingUdfWrapper<string, string>((str) => $"udf2 {str}");
            var udfWrapper3 = new PicklingUdfWrapper<int, int, int>((arg1, arg2) => arg1 + arg2);

            var command1 = new Command()
            {
                ChainedUdfs = new PicklingWorkerFunction.ExecuteDelegate[]
                {
                    udfWrapper1.Execute,
                    udfWrapper2.Execute
                },
                ArgOffsets = new[] { 0 },
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var command2 = new Command()
            {
                ChainedUdfs = new PicklingWorkerFunction.ExecuteDelegate[] {udfWrapper3.Execute },
                ArgOffsets = new[] { 1, 2 },
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            return new CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_BATCHED_UDF,
                Commands = new[] { command1, command2 }
            };
        }
    }
}
