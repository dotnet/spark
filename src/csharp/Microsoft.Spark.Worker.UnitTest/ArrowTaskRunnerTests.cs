// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Command;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class ArrowTaskRunnerTests
    {
        private const string UdfFailureMessage = "Arrow test UDF preparation failed";

        public static IEnumerable<object[]> ArrowPaths()
        {
            foreach (string path in new[]
            {
                "ArrowScalar", "DataFrameScalar", "ArrowGrouped", "DataFrameGrouped"
            })
            {
                yield return new object[] { path, true };
                yield return new object[] { path, false };
            }
        }

        [Theory]
        [MemberData(nameof(ArrowPaths))]
        public void FirstUdfFailureWritesOuterExceptionWithoutStartingArrow(string path, bool legacy)
        {
            using var input = CreateTaskInput(path, legacy, -1);
            using var output = new ArrowFaultOutputStream();
            var socket = new TestSocket(input, output);

            new TaskRunner(0, socket, true, Version(legacy)).Run();

            output.Position = 0;
            Assert.Equal((int)SpecialLengths.PYTHON_EXCEPTION_THROWN, SerDe.ReadInt32(output));
            Assert.Contains(UdfFailureMessage, SerDe.ReadString(output));
            Assert.Equal(output.Length, output.Position);
            Assert.Equal(1, output.FlushCount);
            Assert.Equal(1, socket.DisposeCount);
            Assert.True(input.Position < input.Length);
        }

        [Theory]
        [MemberData(nameof(ArrowPaths))]
        public void LaterUdfFailureHasReadableArrowEosThenOriginalException(string path, bool legacy)
        {
            using var input = CreateTaskInput(path, legacy, 7, -1);
            using var output = new ArrowFaultOutputStream();
            var socket = new TestSocket(input, output);

            new TaskRunner(0, socket, true, Version(legacy)).Run();

            output.Position = 0;
            AssertArrowBatchAndEnd(output, 7, path.EndsWith("Grouped") && !legacy);
            Assert.Equal((int)SpecialLengths.PYTHON_EXCEPTION_THROWN, SerDe.ReadInt32(output));
            Assert.Contains(UdfFailureMessage, SerDe.ReadString(output));
            Assert.Equal(output.Length, output.Position);
            Assert.Equal(1, output.FlushCount);
            Assert.Equal(1, socket.DisposeCount);
        }

        [Theory]
        [MemberData(nameof(ArrowPaths))]
        public void SuccessfulPathReturnsReadableDataAndCompleteTaskTrailer(string path, bool legacy)
        {
            using var input = CreateTaskInput(path, legacy, 7);
            using var output = new ArrowFaultOutputStream();
            var socket = new TestSocket(input, output);

            new TaskRunner(0, socket, true, Version(legacy)).Run();

            output.Position = 0;
            AssertArrowBatchAndEnd(output, 7, path.EndsWith("Grouped") && !legacy);
            AssertSuccessTrailer(output);
            Assert.Equal(output.Length, output.Position);
            Assert.Equal(1, socket.DisposeCount);
        }

        [Theory]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [InlineData(false, false)]
        [InlineData(false, true)]
        public void PartialArrowWriteNeverAppendsExceptionOrSuccessfulTrailer(
            bool legacy,
            bool ioException)
        {
            byte[] inputBytes;
            byte[] healthy;
            long arrowEnd;
            using (var input = CreateTaskInput("ArrowScalar", legacy, 7))
            using (var output = new ArrowFaultOutputStream())
            {
                inputBytes = input.ToArray();
                new TaskRunner(0, new TestSocket(input, output), true, Version(legacy)).Run();
                healthy = output.ToArray();
                output.Position = 0;
                AssertArrowBatchAndEnd(output, 7, false);
                arrowEnd = output.Position;
            }

            // Every byte offset covers START, both IPC length encodings, schema, batch and EOS.
            for (int offset = 0; offset < arrowEnd; ++offset)
            {
                Exception primary = ioException
                    ? new IOException($"partial Arrow write at {offset}")
                    : new InvalidOperationException($"partial Arrow write at {offset}");
                using var input = new MemoryStream(inputBytes);
                using var output = new ArrowFaultOutputStream(offset, primary);
                var socket = new TestSocket(input, output);

                new TaskRunner(0, socket, true, Version(legacy)).Run();

                Assert.Equal(healthy.Take(offset), output.ToArray());
                Assert.True(output.HasFailed);
                Assert.Equal(0, output.ProtocolCallsAfterFailure);
                Assert.Equal(0, output.FlushCount);
                Assert.Equal(1, socket.DisposeCount);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void EveryPartialSuccessTrailerWriteClosesSocketWithoutAppendingException(bool ioException)
        {
            byte[] inputBytes;
            long arrowEnd;
            using (var input = CreateTaskInput("ArrowScalar", false, 7))
            using (var output = new ArrowFaultOutputStream())
            {
                inputBytes = input.ToArray();
                new TaskRunner(0, new TestSocket(input, output), true, Version(false)).Run();
                output.Position = 0;
                AssertArrowBatchAndEnd(output, 7, false);
                arrowEnd = output.Position;
                AssertSuccessTrailer(output);
                Assert.Equal(56, output.Length - arrowEnd);
            }

            // 44 timing bytes, END_OF_DATA_SECTION, accumulator count and END_OF_STREAM.
            for (int offset = 0; offset < 56; ++offset)
            {
                Exception primary = ioException
                    ? new IOException($"partial trailer at {offset}")
                    : new InvalidOperationException($"partial trailer at {offset}");
                using var input = new MemoryStream(inputBytes);
                using var output = new ArrowFaultOutputStream(arrowEnd + offset, primary);
                var socket = new TestSocket(input, output);

                new TaskRunner(0, socket, true, Version(false)).Run();

                Assert.Equal(arrowEnd + offset, output.Length);
                Assert.True(output.HasFailed);
                Assert.Equal(0, output.ProtocolCallsAfterFailure);
                Assert.Equal(0, output.FlushCount);
                Assert.Equal(1, socket.DisposeCount);
            }
        }

        [Fact]
        public void InputFailureDuringSuccessHandshakeDoesNotAppendException()
        {
            using var taskInput = CreateTaskInput("ArrowScalar", false, 7);
            using var input = new FaultInputStream(
                taskInput.ToArray(), taskInput.Length - sizeof(int),
                new InvalidOperationException("handshake read failed"));
            using var output = new ArrowFaultOutputStream();
            var socket = new TestSocket(input, output);

            new TaskRunner(0, socket, true, Version(false)).Run();

            output.Position = 0;
            AssertArrowBatchAndEnd(output, 7, false);
            AssertTimingAndAccumulators(output);
            Assert.Equal(output.Length, output.Position);
            Assert.Equal(1, socket.DisposeCount);
            Assert.Equal(0, output.FlushCount);
            Assert.True(input.HasFailed);
        }

        [Fact]
        public void NewTaskAfterSuccessfulReuseGetsFreshExceptionPermission()
        {
            using var taskInput = CreateTaskInput("ArrowScalar", false, 7);
            using var input = new FaultInputStream(
                taskInput.ToArray(), taskInput.Length,
                new InvalidOperationException("next task read failed"));
            using var output = new ArrowFaultOutputStream();
            var socket = new TestSocket(input, output);

            new TaskRunner(0, socket, true, Version(false)).Run();

            output.Position = 0;
            AssertArrowBatchAndEnd(output, 7, false);
            AssertSuccessTrailer(output);
            Assert.Equal((int)SpecialLengths.PYTHON_EXCEPTION_THROWN, SerDe.ReadInt32(output));
            Assert.Contains("next task read failed", SerDe.ReadString(output));
            Assert.Equal(output.Length, output.Position);
            Assert.Equal(2, output.FlushCount);
            Assert.Equal(1, socket.DisposeCount);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void ExceptionWriteOrFlushFailureIsAttemptedOnceAndPreservesPrimary(bool failFlush)
        {
            var primary = new InvalidOperationException("primary input failure");
            var secondary = new InvalidOperationException("secondary exception report failure");
            using var input = new FaultInputStream(System.Array.Empty<byte>(), 0, primary);
            using var output = failFlush
                ? new ArrowFaultOutputStream { FlushFailure = secondary }
                : new ArrowFaultOutputStream(2, secondary);
            var socket = new TestSocket(input, output);
            var runner = new TaskRunner(0, socket, true, Version(false));
            MethodInfo process = typeof(TaskRunner).GetMethod(
                "ProcessStream", BindingFlags.NonPublic | BindingFlags.Instance);

            // Run catches failures for logging; invoke this private boundary to check identity.
            var wrapped = Assert.Throws<TargetInvocationException>(() =>
                process.Invoke(runner, new object[] { input, output, Version(false), false }));

            Assert.Same(primary, wrapped.InnerException);
            Assert.Equal(failFlush ? 1 : 0, output.FlushCount);
            Assert.Equal(0, output.ProtocolCallsAfterFailure);
            if (failFlush)
            {
                output.Position = 0;
                Assert.Equal((int)SpecialLengths.PYTHON_EXCEPTION_THROWN, SerDe.ReadInt32(output));
                Assert.Contains(primary.Message, SerDe.ReadString(output));
                Assert.Equal(output.Length, output.Position);
            }
            else
            {
                Assert.Equal(2, output.Length);
            }

            using var runInput = new FaultInputStream(System.Array.Empty<byte>(), 0, primary);
            using var runOutput = failFlush
                ? new ArrowFaultOutputStream { FlushFailure = secondary }
                : new ArrowFaultOutputStream(2, secondary);
            var runSocket = new TestSocket(runInput, runOutput);
            new TaskRunner(0, runSocket, true, Version(false)).Run();
            Assert.Equal(1, runSocket.DisposeCount);
            Assert.Equal(0, runOutput.ProtocolCallsAfterFailure);
        }

        [Fact]
        public void SuccessFlushFailureDoesNotAppendAnythingOrReuseSocket()
        {
            using var input = CreateTaskInput("ArrowScalar", false, 7);
            using var output = new ArrowFaultOutputStream
            {
                FlushFailure = new InvalidOperationException("success flush failed")
            };
            var socket = new TestSocket(input, output);

            new TaskRunner(0, socket, true, Version(false)).Run();

            output.Position = 0;
            AssertArrowBatchAndEnd(output, 7, false);
            AssertSuccessTrailer(output);
            Assert.Equal(output.Length, output.Position);
            Assert.Equal(0, output.ProtocolCallsAfterFailure);
            Assert.Equal(1, output.FlushCount);
            Assert.Equal(1, socket.DisposeCount);
        }

        [Theory]
        [InlineData("ArrowScalar")]
        [InlineData("DataFrameScalar")]
        [InlineData("ArrowGrouped")]
        [InlineData("DataFrameGrouped")]
        public void SparkFourStillRejectsArrowBeforeOutput(string path)
        {
            Delegate execute = CreateUdf(path);
            Sql.WorkerFunction worker = execute switch
            {
                Sql.ArrowWorkerFunction.ExecuteDelegate arrow => new Sql.ArrowWorkerFunction(arrow),
                Sql.DataFrameWorkerFunction.ExecuteDelegate frame => new Sql.DataFrameWorkerFunction(frame),
                Sql.ArrowGroupedMapWorkerFunction.ExecuteDelegate grouped => new Sql.ArrowGroupedMapWorkerFunction(grouped),
                Sql.DataFrameGroupedMapWorkerFunction.ExecuteDelegate frameGrouped => new Sql.DataFrameGroupedMapWorkerFunction(frameGrouped),
                _ => throw new InvalidOperationException()
            };
            var command = new Worker.CommandPayload
            {
                EvalType = path.EndsWith("Grouped")
                    ? UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF
                    : UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                Commands = new[]
                {
                    new SqlCommand
                    {
                        ArgOffsets = new[] { 0 },
                        WorkerFunction = worker,
                        NumChainedFunctions = 1,
                        SerializerMode = CommandSerDe.SerializedMode.Row,
                        DeserializerMode = CommandSerDe.SerializedMode.Row
                    }
                }
            };
            using var input = new MemoryStream();
            using var output = new MemoryStream();

            Assert.Throws<NotSupportedException>(() =>
                new CommandExecutor(new Version(4, 0, 4)).Execute(input, output, 0, command));

            Assert.Equal(0, output.Length);
        }

        private static MemoryStream CreateTaskInput(string path, bool legacy, params int[] values)
        {
            var input = new MemoryStream();
            var writer = new PayloadWriter(
                Version(legacy),
                legacy ? new TaskContextWriterV2_4_X() : (ITaskContextWriter)new TaskContextWriterV3_0_X(),
                new BroadcastVariableWriterV2_4_X(),
                new ArrowCommandWriter());
            Payload payload = TestData.GetDefaultPayload();
            payload.BroadcastVariables.DecryptionServerNeeded = false;
            writer.Write(input, payload, new CommandPayload
            {
                EvalType = path.EndsWith("Grouped")
                    ? UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF
                    : UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                Commands = new[]
                {
                    new Command
                    {
                        ArgOffsets = new[] { 0 },
                        ChainedUdfs = new[] { CreateUdf(path) },
                        SerializerMode = CommandSerDe.SerializedMode.Row,
                        DeserializerMode = CommandSerDe.SerializedMode.Row
                    }
                }
            });

            using RecordBatch schemaBatch = ArrowOutputSessionTests.CreateBatch(0);
            using (var arrow = new ArrowStreamWriter(
                input, schemaBatch.Schema, true, ArrowOutputSessionTests.Options(legacy)))
            {
                foreach (int value in values)
                {
                    using RecordBatch batch = ArrowOutputSessionTests.CreateBatch(1, value);
                    arrow.WriteRecordBatch(batch);
                }

                arrow.WriteEnd();
            }

            SerDe.Write(input, (int)SpecialLengths.END_OF_STREAM);
            input.Position = 0;
            return input;
        }

        private static Delegate CreateUdf(string path)
        {
            switch (path)
            {
                case "ArrowScalar":
                    var arrow = new Sql.ArrowUdfWrapper<Int32Array, Int32Array>(ArrowIdentity);
                    return new Sql.ArrowWorkerFunction.ExecuteDelegate(arrow.Execute);
                case "DataFrameScalar":
                    var frame = new Sql.DataFrameUdfWrapper<Int32DataFrameColumn, Int32DataFrameColumn>(DataFrameIdentity);
                    return new Sql.DataFrameWorkerFunction.ExecuteDelegate(frame.Execute);
                case "ArrowGrouped":
                    var grouped = new Sql.ArrowGroupedMapUdfWrapper(ArrowGroupedIdentity);
                    return new Sql.ArrowGroupedMapWorkerFunction.ExecuteDelegate(grouped.Execute);
                case "DataFrameGrouped":
                    var frameGrouped = new Sql.DataFrameGroupedMapUdfWrapper(DataFrameGroupedIdentity);
                    return new Sql.DataFrameGroupedMapWorkerFunction.ExecuteDelegate(frameGrouped.Execute);
                default:
                    throw new ArgumentException(nameof(path));
            }
        }

        private static Int32Array ArrowIdentity(Int32Array values)
        {
            if (values.Length > 0 && values.GetValue(0) < 0)
            {
                throw new InvalidOperationException(UdfFailureMessage);
            }

            return values;
        }

        private static Int32DataFrameColumn DataFrameIdentity(Int32DataFrameColumn values)
        {
            if (values.Length > 0 && values[0] < 0)
            {
                throw new InvalidOperationException(UdfFailureMessage);
            }

            return values;
        }

        private static RecordBatch ArrowGroupedIdentity(RecordBatch batch)
        {
            ArrowIdentity((Int32Array)batch.Column(0));
            return batch;
        }

        private static DataFrame DataFrameGroupedIdentity(DataFrame frame)
        {
            DataFrameIdentity((Int32DataFrameColumn)frame.Columns[0]);
            return frame;
        }

        private static Version Version(bool legacy) =>
            legacy ? new Version(2, 4, 0) : new Version(3, 0, 0);

        private static void AssertArrowBatchAndEnd(Stream output, int expected, bool grouped)
        {
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            using RecordBatch batch = reader.ReadNextRecordBatch();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            IArrowArray column = grouped
                ? Assert.IsType<StructArray>(batch.Column(0)).Fields[0]
                : batch.Column(0);
            Assert.Equal(expected, Assert.IsType<Int32Array>(column).GetValue(0));
            Assert.Null(reader.ReadNextRecordBatch());
        }

        private static void AssertTimingAndAccumulators(Stream output)
        {
            Assert.Equal((int)SpecialLengths.TIMING_DATA, SerDe.ReadInt32(output));
            for (int i = 0; i < 3; ++i)
            {
                Assert.True(SerDe.ReadInt64(output) > 0);
            }

            Assert.Equal(0, SerDe.ReadInt64(output));
            Assert.Equal(0, SerDe.ReadInt64(output));
            Assert.Equal((int)SpecialLengths.END_OF_DATA_SECTION, SerDe.ReadInt32(output));
            Assert.Equal(0, SerDe.ReadInt32(output));
        }

        private static void AssertSuccessTrailer(Stream output)
        {
            AssertTimingAndAccumulators(output);
            Assert.Equal((int)SpecialLengths.END_OF_STREAM, SerDe.ReadInt32(output));
        }

        private sealed class ArrowCommandWriter : CommandWriterBase, ICommandWriter
        {
            public void Write(Stream stream, CommandPayload payload)
            {
                SerDe.Write(stream, (int)payload.EvalType);
                SerDe.Write(stream, 0); // Arrow configuration count.
                Write(stream, payload.Commands);
            }
        }

        private sealed class FaultInputStream : MemoryStream
        {
            private readonly long _failOffset;
            private readonly Exception _failure;

            internal FaultInputStream(byte[] input, long failOffset, Exception failure)
                : base(input)
            {
                _failOffset = failOffset;
                _failure = failure;
            }

            internal bool HasFailed { get; private set; }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (Position >= _failOffset)
                {
                    HasFailed = true;
                    throw _failure;
                }

                return base.Read(buffer, offset, (int)Math.Min(count, _failOffset - Position));
            }

            public override int Read(Span<byte> buffer)
            {
                if (Position >= _failOffset)
                {
                    HasFailed = true;
                    throw _failure;
                }

                return base.Read(buffer.Slice(0, (int)Math.Min(buffer.Length, _failOffset - Position)));
            }
        }

        private sealed class TestSocket : ISocketWrapper
        {
            internal TestSocket(Stream input, Stream output)
            {
                InputStream = input;
                OutputStream = output;
            }

            internal int DisposeCount { get; private set; }
            public Stream InputStream { get; }
            public Stream OutputStream { get; }
            public EndPoint LocalEndPoint => null;
            public EndPoint RemoteEndPoint => null;
            public void Dispose() => ++DisposeCount;
            public ISocketWrapper Accept() => throw new NotSupportedException();
            public void Connect(IPAddress remoteaddr, int port, string secret = null) =>
                throw new NotSupportedException();
            public void Listen(int backlog = 16) => throw new NotSupportedException();
        }
    }
}
