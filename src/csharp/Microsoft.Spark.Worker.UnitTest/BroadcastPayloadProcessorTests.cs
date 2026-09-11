// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Reflection;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Processor;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class BroadcastPayloadProcessorTests
    {
        private static readonly FieldInfo s_rootDirectoryField = typeof(SparkFiles).GetField(
            "s_rootDirectory", BindingFlags.NonPublic | BindingFlags.Static);
        private static readonly FieldInfo s_isRunningOnWorkerField = typeof(SparkFiles).GetField(
            "s_isRunningOnWorker", BindingFlags.NonPublic | BindingFlags.Static);

        [Theory]
        [InlineData(false, null)]
        [InlineData(true, "existing-worker-directory")]
        public void Spark40OuterFrameRestoresSparkFilesState(bool isRunningOnWorker, string rootDirectory)
        {
            object previousRootDirectory = s_rootDirectoryField.GetValue(null);
            object previousIsRunningOnWorker = s_isRunningOnWorkerField.GetValue(null);
            try
            {
                s_rootDirectoryField.SetValue(null, rootDirectory);
                s_isRunningOnWorkerField.SetValue(null, isRunningOnWorker);

                Spark40OuterFramePreservesMultipleBroadcastsAndCommandAlignment();

                Assert.Equal(rootDirectory, s_rootDirectoryField.GetValue(null));
                Assert.Equal(isRunningOnWorker, s_isRunningOnWorkerField.GetValue(null));
            }
            finally
            {
                s_rootDirectoryField.SetValue(null, previousRootDirectory);
                s_isRunningOnWorkerField.SetValue(null, previousIsRunningOnWorker);
            }
        }

        [Fact]
        public void Spark40OuterFramePreservesMultipleBroadcastsAndCommandAlignment()
        {
            long firstId =
                (BitConverter.ToInt64(Guid.NewGuid().ToByteArray(), 0) & (long.MaxValue >> 1)) + 1;
            long secondId = firstId + 1;
            TaskContext previousTaskContext = TaskContextHolder.Get();
            // The public getter can call the JVM before Worker setup. Preserve both
            // thread-static fields so cleanup also restores the original execution mode.
            object previousSparkFilesDir = s_rootDirectoryField.GetValue(null);
            object previousIsRunningOnWorker = s_isRunningOnWorkerField.GetValue(null);
            using var directory = new TemporaryDirectory();

            try
            {
                string firstPath = Path.Combine(directory.Path, "first.broadcast");
                string secondPath = Path.Combine(directory.Path, "second.broadcast");
                using (FileStream file = File.Create(firstPath))
                {
                    BinarySerDe.Serialize(file, 7);
                }

                using (FileStream file = File.Create(secondPath))
                {
                    BinarySerDe.Serialize(file, "second-broadcast");
                }

                var wrapper = new PicklingUdfWrapper<int, long, long, string>(ReadBroadcasts);
                byte[] commandBytes = CommandSerDe.Serialize(
                    (PicklingWorkerFunction.ExecuteDelegate)wrapper.Execute,
                    CommandSerDe.SerializedMode.Row,
                    CommandSerDe.SerializedMode.Row);
                string driverVersion =
                    AssemblyInfoProvider.MicrosoftSparkAssemblyInfo().AssemblyVersion;
                using var stream = new MemoryStream();

                // Spark 4 PythonRunner/PythonWorkerUtils outer fields, written directly
                // so the fixture does not depend on PayloadWriter's zero-broadcast format.
                SerDe.Write(stream, 3); // Split index.
                SerDe.Write(stream, driverVersion);
                SerDe.Write(stream, false); // Barrier task.
                SerDe.Write(stream, 0); // Barrier server port.
                SerDe.Write(stream, ""); // Barrier server secret.
                SerDe.Write(stream, 17); // Stage ID.
                SerDe.Write(stream, 3); // Partition ID.
                SerDe.Write(stream, 2); // Attempt number.
                SerDe.Write(stream, 4294967301L); // Attempt ID exceeds Int32.
                SerDe.Write(stream, 4); // CPUs.
                SerDe.Write(stream, 1); // Resource count.
                SerDe.Write(stream, "gpu"); // Resource key.
                SerDe.Write(stream, "gpu"); // Resource name.
                SerDe.Write(stream, 2); // Resource address count.
                SerDe.Write(stream, "0");
                SerDe.Write(stream, "1");
                SerDe.Write(stream, 2); // Local property count.
                SerDe.Write(stream, "streaming.sql.batchId");
                SerDe.Write(stream, "42");
                SerDe.Write(stream, "spark.job.description");
                SerDe.Write(stream, "broadcast payload");
                SerDe.Write(stream, directory.Path); // SparkFiles root.
                SerDe.Write(stream, 2); // Python include count.
                SerDe.Write(stream, "dependency.zip");
                SerDe.Write(stream, "helpers.dll");
                SerDe.Write(stream, false); // Plaintext broadcasts.
                SerDe.Write(stream, 2); // Broadcast change count.
                SerDe.Write(stream, firstId);
                SerDe.Write(stream, firstPath);
                SerDe.Write(stream, secondId);
                SerDe.Write(stream, secondPath);

                // SQL_BATCHED_UDF with three positional arguments and one function.
                SerDe.Write(stream, 100);
                SerDe.Write(stream, false); // Profiling disabled.
                SerDe.Write(stream, 1); // UDF count.
                SerDe.Write(stream, 3); // Argument count.
                for (int offset = 0; offset < 3; ++offset)
                {
                    SerDe.Write(stream, offset);
                    SerDe.Write(stream, false); // Argument has no name.
                }

                SerDe.Write(stream, 1); // Chained function count.
                SerDe.Write(stream, commandBytes.Length);
                SerDe.Write(stream, commandBytes);
                long frameEnd = stream.Position;
                const int Sentinel = 0x13579bdf;
                SerDe.Write(stream, Sentinel);
                stream.Position = 0;

                Payload payload = new PayloadProcessor(new Version("4.0.4")).Process(stream);

                Assert.Equal(3, payload.SplitIndex);
                Assert.Equal(driverVersion, payload.Version);
                Assert.False(payload.TaskContext.IsBarrier);
                Assert.Equal(0, payload.TaskContext.Port);
                Assert.Equal("", payload.TaskContext.Secret);
                Assert.Equal(17, payload.TaskContext.StageId);
                Assert.Equal(3, payload.TaskContext.PartitionId);
                Assert.Equal(2, payload.TaskContext.AttemptNumber);
                Assert.Equal(4294967301L, payload.TaskContext.AttemptId);
                Assert.Equal(4, payload.TaskContext.CPUs);
                // Resource entries are consumed but are not exposed by the current worker.
                Assert.Empty(payload.TaskContext.Resources);
                Assert.Equal(2, payload.TaskContext.LocalProperties.Count);
                Assert.Equal("42", payload.TaskContext.LocalProperties["streaming.sql.batchId"]);
                Assert.Equal(
                    "broadcast payload",
                    payload.TaskContext.LocalProperties["spark.job.description"]);
                Assert.Same(payload.TaskContext, TaskContextHolder.Get());
                Assert.Equal(directory.Path, payload.SparkFilesDir);
                Assert.Equal(directory.Path, SparkFiles.GetRootDirectory());
                Assert.Equal(new[] { "dependency.zip", "helpers.dll" }, payload.IncludeItems);
                Assert.False(payload.BroadcastVariables.DecryptionServerNeeded);
                Assert.Equal(2, payload.BroadcastVariables.Count);
                Assert.Equal(7, Assert.IsType<int>(BroadcastRegistry.Get(firstId)));
                Assert.Equal(
                    "second-broadcast",
                    Assert.IsType<string>(BroadcastRegistry.Get(secondId)));
                Assert.Equal(UdfUtils.PythonEvalType.SQL_BATCHED_UDF, payload.Command.EvalType);

                SqlCommand command = Assert.IsType<SqlCommand>(
                    Assert.Single(payload.Command.Commands));
                Assert.Equal(new[] { 0, 1, 2 }, command.ArgOffsets);
                Assert.Equal(1, command.NumChainedFunctions);
                Assert.Equal(CommandSerDe.SerializedMode.Row, command.SerializerMode);
                Assert.Equal(CommandSerDe.SerializedMode.Row, command.DeserializerMode);
                var function = Assert.IsType<PicklingWorkerFunction>(command.WorkerFunction);
                Assert.Equal(
                    "12:second-broadcast",
                    function.Func(
                        payload.SplitIndex,
                        new object[] { 5, firstId, secondId },
                        command.ArgOffsets));
                Assert.Equal(frameEnd, stream.Position);
                Assert.Equal(Sentinel, SerDe.ReadInt32(stream));
                Assert.Equal(stream.Length, stream.Position);
            }
            finally
            {
                BroadcastRegistry.Remove(firstId);
                BroadcastRegistry.Remove(secondId);
                TaskContextHolder.Set(previousTaskContext);
                s_rootDirectoryField.SetValue(null, previousSparkFilesDir);
                s_isRunningOnWorkerField.SetValue(null, previousIsRunningOnWorker);
            }
        }

        private static string ReadBroadcasts(int value, long firstId, long secondId)
        {
            int first = (int)BroadcastRegistry.Get(firstId);
            string second = (string)BroadcastRegistry.Get(secondId);
            return $"{value + first}:{second}";
        }
    }
}
