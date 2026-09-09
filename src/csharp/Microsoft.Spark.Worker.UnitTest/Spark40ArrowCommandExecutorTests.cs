// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Command;
using Moq;
using Xunit;
using SqlTypes = Microsoft.Spark.Sql.Types;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class Spark40ArrowCommandExecutorTests
    {
        private const int Sentinel = 0x12345678;
        private static readonly Version s_version = new Version(4, 0, 4);

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void ScalarRoundTripsNullsMultipleBatchesAndAliasedResults(bool dataFrame)
        {
            using var batch = IntBatch(new int?[] { 2, null, 5 });
            using var input = Input(batch, batch);
            using var output = new MemoryStream();
            SqlCommand command = Scalar(dataFrame);
            CommandExecutorStat stat = Execute(input, output, false, command, command);
            Assert.Equal(6, stat.NumEntriesProcessed);
            Assert.Equal(Sentinel, SerDe.ReadInt32(input));
            Assert.Equal(input.Length, input.Position);
            SerDe.Write(output, Sentinel);
            output.Position = 0;
            Assert.Equal(-6, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            for (int i = 0; i < 2; ++i)
            {
                using RecordBatch result = reader.ReadNextRecordBatch();
                Assert.Equal(2, result.ColumnCount);
                foreach (Int32Array column in result.Arrays)
                {
                    Assert.Equal(2, column.GetValue(0));
                    Assert.True(column.IsNull(1));
                    Assert.Equal(5, column.GetValue(2));
                }
            }
            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Equal(Sentinel, SerDe.ReadInt32(output));
            Assert.Equal(output.Length, output.Position);
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public void SchemaOnlyInputDoesNotInvokeUserCodeOrStartOutput(bool dataFrame, bool grouped)
        {
            using var batch = IntBatch(new int?[] { 1 });
            using var input = new MemoryStream();
            using (var writer = new ArrowStreamWriter(input, batch.Schema, leaveOpen: true))
            {
                writer.WriteStart();
                writer.WriteEnd();
            }
            SerDe.Write(input, Sentinel);
            input.Position = 0;
            using var output = new MemoryStream();
            SqlCommand command = grouped ? Group(dataFrame) : Scalar(dataFrame);
            if (grouped)
            {
                command.WorkerFunction = dataFrame ?
                    new Sql.DataFrameGroupedMapWorkerFunction(_ => throw new Exception("must not run")) :
                    (Sql.WorkerFunction)new Sql.ArrowGroupedMapWorkerFunction(_ => throw new Exception("must not run"));
            }
            else
            {
                command.WorkerFunction = dataFrame ?
                    new Sql.DataFrameWorkerFunction((_, __) => throw new Exception("must not run")) :
                    (Sql.WorkerFunction)new Sql.ArrowWorkerFunction((_, __) => throw new Exception("must not run"));
            }
            Assert.Equal(0, Execute(input, output, grouped, command).NumEntriesProcessed);
            Assert.Equal(0, output.Length);
            Assert.Equal(Sentinel, SerDe.ReadInt32(input));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void RealEmptyScalarBatchIsStillInvoked(bool dataFrame)
        {
            using var batch = IntBatch(System.Array.Empty<int?>());
            using var input = Input(batch);
            using var output = new MemoryStream();
            Execute(input, output, false, Scalar(dataFrame));
            output.Position = 0;
            Assert.Equal(-6, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            using RecordBatch result = reader.ReadNextRecordBatch();
            Assert.Equal(0, result.Length);
            Assert.Null(reader.ReadNextRecordBatch());
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public void InvalidScalarOutputFailsBeforeStart(bool dataFrame, bool nullResult)
        {
            using var batch = IntBatch(new int?[] { 2, 3 });
            using var input = Input(batch);
            using var output = new MemoryStream();
            SqlCommand command = Scalar(dataFrame);
            command.WorkerFunction = dataFrame ?
                new Sql.DataFrameWorkerFunction((_, __) => nullResult ? null : new Int32DataFrameColumn("short", 1)) :
                (Sql.WorkerFunction)new Sql.ArrowWorkerFunction((_, __) => nullResult ? null :
                    new Int32Array.Builder().Append(1).Build());
            Assert.Throws<InvalidDataException>(() => Execute(input, output, false, command));
            Assert.Equal(0, output.Length);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GroupedMapProjectsValuesAndPreservesOrder(bool dataFrame)
        {
            using var batch = IntBatch(new int?[] { 10, 20 }, new int?[] { 1, 2 }, new int?[] { 7, 8 });
            using var input = Input(batch, batch);
            using var output = new MemoryStream();
            SqlCommand command = Group(dataFrame, 2);
            command.GroupingKeyOffsets = new[] { 0, 0 };
            command.ArgOffsets = new[] { 2, 1 };
            Execute(input, output, true, command);
            output.Position = 0;
            Assert.Equal(-6, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            for (int i = 0; i < 2; ++i)
            {
                using RecordBatch result = reader.ReadNextRecordBatch();
                var value = Assert.IsType<StructArray>(result.Column(0));
                Assert.Equal(7, ((Int32Array)value.Fields[0]).GetValue(0));
                Assert.Equal(1, ((Int32Array)value.Fields[1]).GetValue(0));
            }
            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Equal(Sentinel, SerDe.ReadInt32(input));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GroupedMapAcceptsWideTablesAndNoGroupingKeys(bool dataFrame)
        {
            int?[][] values = Enumerable.Range(0, 12).Select(i => new int?[] { i }).ToArray();
            using var batch = IntBatch(values);
            using var input = Input(batch);
            using var output = new MemoryStream();
            Execute(input, output, true, Group(dataFrame, 12));
            output.Position = 0;
            Assert.Equal(-6, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            using RecordBatch result = reader.ReadNextRecordBatch();
            var value = Assert.IsType<StructArray>(result.Column(0));
            Assert.Equal(12, value.Fields.Count);
            Assert.Equal(11, ((Int32Array)value.Fields[11]).GetValue(0));
        }

        [Theory]
        [InlineData(false, 0)]
        [InlineData(false, 3)]
        [InlineData(true, 0)]
        [InlineData(true, 3)]
        public void GroupedMapPreservesRowCountForZeroValueColumns(bool dataFrame, int rows)
        {
            using var batch = IntBatch(Enumerable.Repeat<int?>(1, rows).ToArray());
            using var input = Input(batch);
            using var output = new MemoryStream();
            SqlCommand command = Group(dataFrame);
            command.ArgOffsets = System.Array.Empty<int>();
            command.GroupingKeyOffsets = new[] { 0 };
            command.WorkerFunction = dataFrame ?
                new Sql.DataFrameGroupedMapWorkerFunction(projected =>
                {
                    Assert.Empty(projected.Columns);
                    Assert.Equal(rows, projected.Rows.Count);
                    return new FxDataFrame(new Int32DataFrameColumn(
                        "count", new[] { (int)projected.Rows.Count * 2 + 1 }));
                }) :
                (Sql.WorkerFunction)new Sql.ArrowGroupedMapWorkerFunction(projected =>
                {
                    Assert.Equal(0, projected.ColumnCount);
                    Assert.Equal(rows, projected.Length);
                    return IntBatch(new int?[] { projected.Length * 2 + 1 });
                });
            Assert.Equal(1, Execute(input, output, true, command).NumEntriesProcessed);
            Assert.Equal(Sentinel, SerDe.ReadInt32(input));
            SerDe.Write(output, Sentinel);
            output.Position = 0;
            Assert.Equal(-6, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            using RecordBatch result = reader.ReadNextRecordBatch();
            var value = Assert.IsType<StructArray>(result.Column(0));
            Assert.Equal(1, result.Length);
            Assert.Equal(rows * 2 + 1, ((Int32Array)value.Fields[0]).GetValue(0));
            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Equal(Sentinel, SerDe.ReadInt32(output));
        }

        [Theory]
        [InlineData(false, 0)]
        [InlineData(false, 3)]
        [InlineData(true, 0)]
        [InlineData(true, 3)]
        public void GroupedMapReportsPinnedArrowEmptyStructLimitation(bool dataFrame, int rows)
        {
            using var batch = IntBatch(new int?[] { 1, 2 });
            using var input = Input(batch);
            using var output = new MemoryStream();
            SqlCommand command = Group(dataFrame);
            command.ReturnSchema = new SqlTypes.StructType(System.Array.Empty<SqlTypes.StructField>());
            command.WorkerFunction = dataFrame ?
                new Sql.DataFrameGroupedMapWorkerFunction(_ =>
                {
                    var result = new FxDataFrame();
                    for (int i = 0; i < rows; ++i)
                    {
                        result.Append(System.Array.Empty<object>(), inPlace: true);
                    }
                    Assert.Equal(rows, result.Rows.Count);
                    return result;
                }) :
                (Sql.WorkerFunction)new Sql.ArrowGroupedMapWorkerFunction(_ => new RecordBatch(
                    new Schema(System.Array.Empty<Field>(), null),
                    System.Array.Empty<IArrowArray>(), rows));
            // Arrow 14.0.2 NestedType rejects empty fields. Keep this existing dependency
            // boundary explicit; do not fabricate a field or start a malformed output stream.
            ArgumentNullException error = Assert.Throws<ArgumentNullException>(
                () => Execute(input, output, true, command));
            Assert.Equal("fields", error.ParamName);
            Assert.Equal(0, output.Length);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GroupedMapAcceptsZeroRowResults(bool dataFrame)
        {
            using var batch = IntBatch(new int?[] { 1, 2 });
            using var input = Input(batch);
            using var output = new MemoryStream();
            SqlCommand command = Group(dataFrame);
            command.WorkerFunction = dataFrame ?
                new Sql.DataFrameGroupedMapWorkerFunction(_ => new FxDataFrame(new Int32DataFrameColumn("renamed", 0))) :
                (Sql.WorkerFunction)new Sql.ArrowGroupedMapWorkerFunction(_ => IntBatch(System.Array.Empty<int?>()));
            Execute(input, output, true, command);
            output.Position = 0;
            Assert.Equal(-6, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            using RecordBatch result = reader.ReadNextRecordBatch();
            Assert.Equal(0, result.Length);
            Assert.Null(reader.ReadNextRecordBatch());
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void DataFrameGroupedNullabilityIsStableAcrossGroups(bool nullFirst)
        {
            int? firstValue = nullFirst ? null : (int?)19;
            int? secondValue = nullFirst ? (int?)19 : null;
            using var first = IntBatch(new[] { firstValue });
            using var second = IntBatch(new[] { secondValue });

            // Microsoft.Data.Analysis derives Arrow field nullability from actual NullCount,
            // not the Apply declaration. Establish that the two input groups trigger this.
            using RecordBatch firstConverted = FxDataFrame.FromArrowRecordBatch(first)
                .ToArrowRecordBatches().Single();
            using RecordBatch secondConverted = FxDataFrame.FromArrowRecordBatch(second)
                .ToArrowRecordBatches().Single();
            Assert.NotEqual(firstConverted.Schema.GetFieldByIndex(0).IsNullable,
                secondConverted.Schema.GetFieldByIndex(0).IsNullable);

            using var input = Input(first, second);
            using var output = new MemoryStream();
            Execute(input, output, true, Group(dataFrame: true));
            SerDe.Write(output, Sentinel);
            output.Position = 0;
            Assert.Equal(-6, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            foreach (int? expected in new[] { firstValue, secondValue })
            {
                using RecordBatch result = reader.ReadNextRecordBatch();
                var values = Assert.IsType<StructArray>(result.Column(0));
                Assert.True(((StructType)values.Data.DataType).Fields[0].IsNullable);
                Assert.Equal(expected, ((Int32Array)values.Fields[0]).GetValue(0));
            }
            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Equal(Sentinel, SerDe.ReadInt32(output));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void DataFrameGroupedNullabilityStillEnforcesTheDeclaration(bool nullFirst)
        {
            using var first = IntBatch(new int?[] { nullFirst ? null : (int?)19 });
            using var second = IntBatch(new int?[] { nullFirst ? (int?)19 : null });
            using var input = Input(first, second);
            using var output = new MemoryStream();
            SqlCommand command = Group(dataFrame: true);
            command.ReturnSchema = new SqlTypes.StructType(new[]
            {
                new SqlTypes.StructField("declared", new SqlTypes.IntegerType(), isNullable: false)
            });
            InvalidDataException error = Assert.Throws<InvalidDataException>(
                () => Execute(input, output, true, command));
            Assert.Contains("non-nullable", error.Message);
            if (nullFirst)
            {
                Assert.Equal(0, output.Length);
            }
            else
            {
                SerDe.Write(output, Sentinel);
                output.Position = 0;
                Assert.Equal(-6, SerDe.ReadInt32(output));
                using var reader = new ArrowStreamReader(output, leaveOpen: true);
                Assert.NotNull(reader.ReadNextRecordBatch());
                Assert.Null(reader.ReadNextRecordBatch());
                Assert.Equal(Sentinel, SerDe.ReadInt32(output));
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GroupedOffsetsAreValidatedBeforeDelegate(bool keyOffset)
        {
            using var batch = IntBatch(new int?[] { 1 });
            using var input = Input(batch);
            using var output = new MemoryStream();
            SqlCommand command = Group(false);
            if (keyOffset)
            {
                command.GroupingKeyOffsets = new[] { 1 };
            }
            else
            {
                command.ArgOffsets = new[] { -1 };
            }
            command.WorkerFunction = new Sql.ArrowGroupedMapWorkerFunction(_ => throw new Exception("must not run"));
            Assert.Throws<InvalidDataException>(() => Execute(input, output, true, command));
            Assert.Equal(0, output.Length);
        }

        [Fact]
        public void SchemaChangeEndsIpcBeforeRethrowing()
        {
            using var batch = IntBatch(new int?[] { 1 });
            using var input = Input(batch, batch);
            using var output = new MemoryStream();
            int calls = 0;
            SqlCommand command = Scalar(false);
            command.WorkerFunction = new Sql.ArrowWorkerFunction((_, __) => ++calls == 1 ?
                new Int32Array.Builder().Append(1).Build() :
                (IArrowArray)new Int64Array.Builder().Append(2).Build());
            Assert.Throws<InvalidDataException>(() => Execute(input, output, false, command));
            SerDe.Write(output, Sentinel);
            output.Position = 0;
            Assert.Equal(-6, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            Assert.NotNull(reader.ReadNextRecordBatch());
            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Equal(Sentinel, SerDe.ReadInt32(output));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TaskRunnerWritesExceptionAtTheCorrectIpcBoundary(bool laterBatch)
        {
            using var input = TaskInput(laterBatch ? new[] { 1, -1 } : new[] { -1 });
            using var output = new MemoryStream();
            var socket = new Mock<ISocketWrapper>();
            socket.SetupGet(s => s.InputStream).Returns(input);
            socket.SetupGet(s => s.OutputStream).Returns(output);
            new TaskRunner(0, socket.Object, reuseSocket: true, s_version).Run();
            socket.Verify(s => s.Dispose(), Times.Once);
            output.Position = 0;
            if (laterBatch)
            {
                Assert.Equal(-6, SerDe.ReadInt32(output));
                using var reader = new ArrowStreamReader(output, leaveOpen: true);
                using RecordBatch result = reader.ReadNextRecordBatch();
                Assert.Equal(1, ((Int32Array)result.Column(0)).GetValue(0));
                Assert.Null(reader.ReadNextRecordBatch());
            }
            Assert.Equal(-2, SerDe.ReadInt32(output));
            Assert.Contains("test udf failed", SerDe.ReadString(output));
            Assert.Equal(output.Length, output.Position);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TaskRunnerDoesNotAppendControlFramesAfterWriteOrFlushFailure(bool flushFailure)
        {
            using var input = TaskInput(new[] { 1 });
            using var output = new FaultStream(flushFailure);
            var socket = new Mock<ISocketWrapper>();
            socket.SetupGet(s => s.InputStream).Returns(input);
            socket.SetupGet(s => s.OutputStream).Returns(output);
            new TaskRunner(0, socket.Object, reuseSocket: true, s_version).Run();
            socket.Verify(s => s.Dispose(), Times.Once);
            Assert.True(output.Failed);
            Assert.Equal(0, output.WritesAfterFailure);
            Assert.True(input.Position < input.Length);
        }

        [Fact]
        public void EosFailureDoesNotMaskTheUserException()
        {
            using var batch = IntBatch(new int?[] { 1 });
            using var input = Input(batch, batch);
            using var output = new FaultStream(flushFailure: true);
            var primary = new InvalidOperationException("original user failure");
            int calls = 0;
            SqlCommand command = Scalar(false);
            command.WorkerFunction = new Sql.ArrowWorkerFunction((columns, offsets) =>
                ++calls == 1 ? columns.Span[offsets[0]] : throw primary);
            ArrowStreamWriteException error = Assert.Throws<ArrowStreamWriteException>(
                () => Execute(input, output, false, command));
            Assert.Same(primary, error.InnerException);
            Assert.Equal(0, output.WritesAfterFailure);
        }

        private static SqlCommand Scalar(bool dataFrame) => new SqlCommand
        {
            SerializerMode = CommandSerDe.SerializedMode.Row,
            DeserializerMode = CommandSerDe.SerializedMode.Row,
            ArgOffsets = new[] { 0 },
            WorkerFunction = dataFrame ?
                new Sql.DataFrameWorkerFunction((columns, offsets) => columns.Span[offsets[0]]) :
                (Sql.WorkerFunction)new Sql.ArrowWorkerFunction((columns, offsets) => columns.Span[offsets[0]])
        };

        private static SqlCommand Group(bool dataFrame, int columns = 1) => new SqlCommand
        {
            SerializerMode = CommandSerDe.SerializedMode.Row,
            DeserializerMode = CommandSerDe.SerializedMode.Row,
            ArgOffsets = Enumerable.Range(0, columns).ToArray(),
            GroupingKeyOffsets = System.Array.Empty<int>(),
            ReturnSchema = new SqlTypes.StructType(Enumerable.Range(0, columns)
                .Select(i => new SqlTypes.StructField("declared" + i, new SqlTypes.IntegerType())).ToArray()),
            WorkerFunction = dataFrame ?
                new Sql.DataFrameGroupedMapWorkerFunction(input => input) :
                (Sql.WorkerFunction)new Sql.ArrowGroupedMapWorkerFunction(input => input)
        };

        private static CommandExecutorStat Execute(
            Stream input, Stream output, bool grouped, params SqlCommand[] commands) =>
            SqlCommandExecutor.Execute(s_version, input, output, grouped ?
                UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF :
                UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF, commands);

        private static RecordBatch IntBatch(params int?[][] columns)
        {
            var arrays = columns.Select(values =>
            {
                var builder = new Int32Array.Builder();
                foreach (int? value in values)
                {
                    if (value.HasValue)
                    {
                        builder.Append(value.Value);
                    }
                    else
                    {
                        builder.AppendNull();
                    }
                }
                return (IArrowArray)builder.Build();
            }).ToArray();
            var fields = columns.Select((_, i) => new Field("input" + i, Int32Type.Default, true));
            return new RecordBatch(new Schema(fields, null), arrays, columns[0].Length);
        }

        private static MemoryStream Input(params RecordBatch[] batches)
        {
            var stream = new MemoryStream();
            using (var writer = new ArrowStreamWriter(stream, batches[0].Schema, leaveOpen: true))
            {
                foreach (RecordBatch batch in batches)
                {
                    writer.WriteRecordBatch(batch);
                }
                writer.WriteEnd();
            }
            SerDe.Write(stream, Sentinel);
            stream.Position = 0;
            return stream;
        }

        private static MemoryStream TaskInput(int[] values)
        {
            var stream = new MemoryStream();
            SerDe.Write(stream, 0);
            SerDe.Write(stream, AssemblyInfoProvider.MicrosoftSparkAssemblyInfo().AssemblyVersion);
            new TaskContextWriterV3_3_X().Write(stream, new TaskContext { Secret = string.Empty });
            SerDe.Write(stream, Path.GetTempPath());
            SerDe.Write(stream, 0); // Includes.
            SerDe.Write(stream, false); // No encrypted broadcasts.
            SerDe.Write(stream, 0);
            SerDe.Write(stream, 200);
            SerDe.Write(stream, 0); // Arrow configuration.
            SerDe.Write(stream, false); // Profiling.
            SerDe.Write(stream, 1); // UDF count.
            SerDe.Write(stream, 1); // Argument count.
            SerDe.Write(stream, 0); // Offset.
            SerDe.Write(stream, false); // No named argument.
            SerDe.Write(stream, 1); // Chain count.
            var wrapper = new Sql.ArrowUdfWrapper<Int32Array, Int32Array>(IdentityOrThrow);
            byte[] command = CommandSerDe.Serialize(
                (Sql.ArrowWorkerFunction.ExecuteDelegate)wrapper.Execute,
                CommandSerDe.SerializedMode.Row, CommandSerDe.SerializedMode.Row);
            SerDe.Write(stream, command.Length);
            SerDe.Write(stream, command);
            using var first = IntBatch(new int?[] { values[0] });
            using (var writer = new ArrowStreamWriter(stream, first.Schema, leaveOpen: true))
            {
                foreach (int value in values)
                {
                    using var batch = IntBatch(new int?[] { value });
                    writer.WriteRecordBatch(batch);
                }
                writer.WriteEnd();
            }
            SerDe.Write(stream, -4);
            stream.Position = 0;
            return stream;
        }

        private static Int32Array IdentityOrThrow(Int32Array input) =>
            input.GetValue(0) < 0 ? throw new InvalidOperationException("test udf failed") : input;

        private sealed class FaultStream : MemoryStream
        {
            private readonly bool _flushFailure;

            internal FaultStream(bool flushFailure)
            {
                _flushFailure = flushFailure;
            }

            internal bool Failed { get; private set; }

            internal int WritesAfterFailure { get; private set; }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (Failed)
                {
                    ++WritesAfterFailure;
                    throw new IOException("write after failure");
                }
                if (!_flushFailure && Length + count > 24)
                {
                    base.Write(buffer, offset, (int)(24 - Length));
                    Failed = true;
                    throw new IOException("partial write");
                }
                base.Write(buffer, offset, count);
            }

            public override void Write(ReadOnlySpan<byte> buffer)
            {
                Write(buffer.ToArray(), 0, buffer.Length);
            }

            public override void Flush()
            {
                if (_flushFailure)
                {
                    Failed = true;
                    throw new IOException("flush failure");
                }
                base.Flush();
            }
        }
    }
}
