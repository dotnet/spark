// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Command;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class DataFrameGroupedMapOutputTests
    {
        [Theory]
        [InlineData(2, false)]
        [InlineData(2, true)]
        [InlineData(3, false)]
        [InlineData(3, true)]
        public void GroupedStringsKeepSchemaAcrossEmptyAndNullGroups(int major, bool emptyFirst)
        {
            var options = new IpcOptions { WriteLegacyIpcFormat = major == 2 };
            var version = new Version(major, 3, 0);
            var originalArrow = new StringArray.Builder().Append("原值🙂").Build();
            using var originalBatch = new RecordBatch(
                new Schema(new[] { new Field("Description", StringType.Default, false) }, null),
                new[] { originalArrow }, 1);
            var outputs = new List<DataFrame>
            {
                new DataFrame(new StringDataFrameColumn("Description",
                    emptyFirst ? new string[0] : new[] { "this is description two150" })),
                DataFrame.FromArrowRecordBatch(originalBatch),
                new DataFrame(new StringDataFrameColumn("Description", new[] { (string)null, "" }))
            };
            int call = 0;
            var command = new SqlCommand
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.DataFrameGroupedMapWorkerFunction(_ => outputs[call++]),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            using var input = CreateInput(3, options);
            using var output = new MemoryStream();
            var context = new ArrowOutputContext();
            CommandExecutorStat stat = SqlCommandExecutor.Execute(
                version, input, output, UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                new[] { command }, context);

            Assert.Equal(3, call);
            Assert.Equal(emptyFirst ? 3 : 4, stat.NumEntriesProcessed);
            Assert.Equal(ArrowOutputPhase.Ended, context.Phase);
            output.Position = 0;
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            for (int group = 0; group < outputs.Count; ++group)
            {
                using RecordBatch batch = reader.ReadNextRecordBatch();
                Assert.NotNull(batch);
                Field field;
                StringArray values;
                if (major == 3)
                {
                    Assert.Single(batch.Schema.FieldsList);
                    Assert.Equal("Struct", batch.Schema.GetFieldByIndex(0).Name);
                    var wrapped = Assert.IsType<StructArray>(batch.Column(0));
                    values = Assert.IsType<StringArray>(wrapped.Fields[0]);
                    field = ((StructType)wrapped.Data.DataType).Fields[0];
                }
                else
                {
                    field = batch.Schema.GetFieldByIndex(0);
                    values = Assert.IsType<StringArray>(batch.Column(0));
                }

                Assert.Equal("Description", field.Name);
                Assert.True(field.IsNullable);
                Assert.Equal(ArrowTypeId.String, field.DataType.TypeId);
                DataFrameColumn expected = outputs[group].Columns[0];
                Assert.Equal(expected.Length, (long)values.Length);
                for (int row = 0; row < values.Length; ++row)
                {
                    Assert.Equal(expected[row] == null, values.IsNull(row));
                    // Arrow 14 GetString confuses an empty data buffer with a null value.
                    // Verify the wire validity and UTF-8 payload independently instead.
                    string actual = values.IsNull(row) ? null :
                        Encoding.UTF8.GetString(values.GetBytes(row).ToArray());
                    Assert.Equal(expected[row], actual);
                }
            }

            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Equal(output.Length, output.Position);
            Assert.Equal("原值🙂", outputs[1].Columns[0][0]);
            Assert.Null(outputs[2].Columns[0][0]);
        }

        [Fact]
        public void GroupedConversionErrorAfterOneGroupClosesArrowBeforeRethrowing()
        {
            var failureFrame = new DataFrame(
                new StringDataFrameColumn("text", new[] { "value" }),
                new UInt32DataFrameColumn("unsupported", new uint[] { uint.MaxValue }));
            int calls = 0;
            var command = new SqlCommand
            {
                ArgOffsets = new[] { 0 },
                WorkerFunction = new Sql.DataFrameGroupedMapWorkerFunction(_ =>
                    ++calls == 1 ?
                        new DataFrame(new StringDataFrameColumn("text", new[] { "first" })) :
                        failureFrame),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };
            var options = new IpcOptions();
            using var input = CreateInput(3, options);
            using var output = new MemoryStream();
            var context = new ArrowOutputContext();
            NotSupportedException error = Assert.Throws<NotSupportedException>(() =>
                SqlCommandExecutor.Execute(new Version(3, 3, 4), input, output,
                    UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                    new[] { command }, context));
            Assert.Contains("unsupported", error.Message);
            Assert.Equal(2, calls);
            Assert.True(context.CanWriteException);
            Assert.Equal(ArrowOutputPhase.Ended, context.Phase);
            output.Position = 0;
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            using RecordBatch first = reader.ReadNextRecordBatch();
            var fields = Assert.IsType<StructArray>(first.Column(0));
            Assert.Equal("first", Assert.IsType<StringArray>(fields.Fields[0]).GetString(0));
            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Equal(output.Length, output.Position);
        }

        [Fact]
        public void Spark4ArrowRemainsUnsupportedBeforeOutput()
        {
            bool called = false;
            var command = new SqlCommand
            {
                WorkerFunction = new Sql.DataFrameGroupedMapWorkerFunction(_ =>
                {
                    called = true;
                    return new DataFrame(new StringDataFrameColumn("text"));
                }),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };
            using var input = new MemoryStream();
            using var output = new MemoryStream();
            var context = new ArrowOutputContext();
            Assert.Throws<NotSupportedException>(() => SqlCommandExecutor.Execute(
                new Version(4, 0, 0), input, output,
                UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF, new[] { command }, context));
            Assert.False(called);
            Assert.Equal(0, output.Length);
            Assert.True(context.CanWriteException);
        }

        private static MemoryStream CreateInput(int groups, IpcOptions options)
        {
            var stream = new MemoryStream();
            var schema = new Schema(new[] { new Field("id", Int32Type.Default, false) }, null);
            using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true, options))
            {
                for (int group = 0; group < groups; ++group)
                {
                    var array = new Int32Array.Builder().Append(group).Build();
                    using var batch = new RecordBatch(schema, new[] { array }, 1);
                    writer.WriteRecordBatch(batch);
                }

                writer.WriteEnd();
            }

            stream.Position = 0;
            return stream;
        }
    }
}
