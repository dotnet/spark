// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class Spark40ArrowCommandSerDeTests
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GroupedSchemaTailPreservesLegacyEnvelopeBytes(bool dataFrame)
        {
            Delegate wrapper = GroupedWrapper(dataFrame);
            byte[] legacy = CommandSerDe.Serialize(wrapper,
                CommandSerDe.SerializedMode.Row, CommandSerDe.SerializedMode.Row);
            var schema = new StructType(new[] { new StructField("value", new IntegerType(), false) });
            byte[] extended = CommandSerDe.SerializeSpark40GroupedMap(wrapper, schema);
            Assert.Equal(extended, CommandSerDe.SerializeSpark40GroupedMap(wrapper, schema.Json));
            Assert.Equal(legacy, extended.Take(legacy.Length));
            using var stream = new MemoryStream(extended);
            stream.Position = legacy.Length;
            Assert.Equal(schema.Json, SerDe.ReadString(stream));
            Assert.Equal(stream.Length, stream.Position);
            Assert.Equal(dataFrame, CommandSerDe.PreflightSpark40Arrow(extended, 20, true,
                out StructType parsed, out bool isRepl));
            Assert.Equal(schema.Json, parsed.Json);
            Assert.False(isRepl);
            if (dataFrame)
            {
                Assert.NotNull(CommandSerDe.DeserializeSpark40Arrow<
                    DataFrameGroupedMapWorkerFunction.ExecuteDelegate>(
                        extended, 20, true, out _, out _));
            }
            else
            {
                Assert.NotNull(CommandSerDe.DeserializeSpark40Arrow<
                    ArrowGroupedMapWorkerFunction.ExecuteDelegate>(
                        extended, 20, true, out _, out _));
            }
        }

        [Fact]
        public void MissingGroupedSchemaIsRejected()
        {
            byte[] command = CommandSerDe.Serialize(GroupedWrapper(false),
                CommandSerDe.SerializedMode.Row, CommandSerDe.SerializedMode.Row);
            Assert.Throws<InvalidDataException>(() =>
                CommandSerDe.PreflightSpark40Arrow(command, 1, true, out _, out _));
        }

        [Fact]
        public void ColumnMetadataIsNotInterpretedAsADataType()
        {
            const string Json = "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\"," +
                "\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"type\":\"udt\"}}]}";
            Assert.False(CommandSerDe.PreflightSpark40Arrow(WithSchema(Json), 1, true,
                out StructType schema, out _));
            Assert.Equal("udt", (string)schema.Fields[0].Metadata["type"]);
        }

        [Theory]
        [InlineData("")]
        [InlineData("{")]
        [InlineData("\"integer\"")]
        [InlineData("{\"type\":\"struct\",\"fields\":[]}{ }")]
        [InlineData("{\"type\":\"struct\",\"type\":\"struct\",\"fields\":[]}")]
        [InlineData("{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"variant\",\"nullable\":true,\"metadata\":{}}]}")]
        [InlineData("{\"type\":\"udt\",\"class\":\"anything\",\"sqlType\":{\"type\":\"struct\",\"fields\":[]}}")]
        public void MalformedOrUnsupportedSchemaIsRejectedBeforeDelegateResolution(string schema)
        {
            byte[] command = WithSchema(schema);
            Assert.Throws<InvalidDataException>(() =>
                CommandSerDe.PreflightSpark40Arrow(command, 1, true, out _, out _));
        }

        [Fact]
        public void GroupedSchemaDepthAndSizeAreBounded()
        {
            string nested = "\"integer\"";
            for (int i = 0; i < 70; ++i)
            {
                nested = "{\"type\":\"array\",\"elementType\":" + nested +
                    ",\"containsNull\":true}";
            }

            byte[] deep = WithSchema("{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":" +
                nested + ",\"nullable\":true,\"metadata\":{}}]}");
            Assert.Throws<InvalidDataException>(() =>
                CommandSerDe.PreflightSpark40Arrow(deep, 1, true, out _, out _));
            byte[] large = WithSchema(new string(' ', (1024 * 1024) + 1));
            Assert.Throws<InvalidDataException>(() =>
                CommandSerDe.PreflightSpark40Arrow(large, 1, true, out _, out _));
        }

        [Fact]
        public void GroupedSchemaMustBeCompleteStrictUtf8AndHaveNoTrailingBytes()
        {
            byte[] valid = WithSchema("{\"type\":\"struct\",\"fields\":[]}");
            byte[] truncated = valid.Take(valid.Length - 1).ToArray();
            byte[] trailing = valid.Concat(new byte[] { 0 }).ToArray();
            byte[] invalidUtf8 = (byte[])valid.Clone();
            invalidUtf8[invalidUtf8.Length - 1] = 0xff;
            foreach (byte[] command in new[] { truncated, trailing, invalidUtf8 })
            {
                Assert.Throws<InvalidDataException>(() =>
                    CommandSerDe.PreflightSpark40Arrow(command, 1, true, out _, out _));
            }
        }

        [Fact]
        public void EvalSpecificWrapperAllowlistsRemainSeparate()
        {
            PicklingWorkerFunction.ExecuteDelegate pickling =
                new PicklingUdfWrapper<int, int>(value => value).Execute;
            ArrowWorkerFunction.ExecuteDelegate arrow =
                new ArrowUdfWrapper<Int32Array, Int32Array>(value => value).Execute;
            byte[] picklingBytes = CommandSerDe.Serialize(pickling,
                CommandSerDe.SerializedMode.Row, CommandSerDe.SerializedMode.Row);
            byte[] arrowBytes = CommandSerDe.Serialize(arrow,
                CommandSerDe.SerializedMode.Row, CommandSerDe.SerializedMode.Row);
            Assert.Throws<InvalidDataException>(() =>
                CommandSerDe.PreflightSpark40Arrow(picklingBytes, 1, false, out _, out _));
            Assert.Throws<InvalidDataException>(() => CommandSerDe.PreflightSpark40(arrowBytes, 1));
            Assert.Throws<InvalidDataException>(() => CommandSerDe.PreflightSpark40Arrow(
                WithSchema("{\"type\":\"struct\",\"fields\":[]}", arrow),
                1, true, out _, out _));
            Assert.Throws<InvalidDataException>(() => CommandSerDe.PreflightSpark40Arrow(
                WithSchema("{\"type\":\"struct\",\"fields\":[]}"),
                1, false, out _, out _));
            Assert.False(CommandSerDe.PreflightSpark40Arrow(
                arrowBytes, 1, false, out _, out _));
            Assert.Throws<InvalidDataException>(() => CommandSerDe.PreflightSpark40Arrow(
                arrowBytes, 2, false, out _, out _));
        }

        [Fact]
        public void ReplIsPreflightedWithoutResolvingItsUserTypes()
        {
            ArrowWorkerFunction.ExecuteDelegate wrapper =
                new ArrowUdfWrapper<Int32Array, Int32Array>(value => value).Execute;
            byte[] normal = CommandSerDe.Serialize(wrapper,
                CommandSerDe.SerializedMode.Row, CommandSerDe.SerializedMode.Row);
            using var stream = new MemoryStream();
            stream.Write(normal, 0, 18);
            stream.WriteByte((byte)'R');
            SerDe.Write(stream, "compilation-directory");
            stream.Write(normal, 19, normal.Length - 19);
            byte[] command = stream.ToArray();
            Assert.False(CommandSerDe.PreflightSpark40Arrow(command, 1, false,
                out _, out bool isRepl));
            Assert.True(isRepl);
            Assert.Throws<NotSupportedException>(() =>
                CommandSerDe.DeserializeSpark40Arrow<ArrowWorkerFunction.ExecuteDelegate>(
                    command, 1, false, out _, out _));
        }

        private static Delegate GroupedWrapper(bool dataFrame) => dataFrame ?
            (Delegate)(DataFrameGroupedMapWorkerFunction.ExecuteDelegate)
                new DataFrameGroupedMapUdfWrapper(value => value).Execute :
            (ArrowGroupedMapWorkerFunction.ExecuteDelegate)
                new ArrowGroupedMapUdfWrapper(value => value).Execute;

        private static byte[] WithSchema(string schema, Delegate wrapper = null)
        {
            byte[] original = CommandSerDe.Serialize(wrapper ?? GroupedWrapper(false),
                CommandSerDe.SerializedMode.Row, CommandSerDe.SerializedMode.Row);
            using var stream = new MemoryStream();
            stream.Write(original, 0, original.Length);
            SerDe.Write(stream, schema);
            return stream.ToArray();
        }
    }
}
