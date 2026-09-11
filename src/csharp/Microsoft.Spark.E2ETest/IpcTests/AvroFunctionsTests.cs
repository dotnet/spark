// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using static Microsoft.Spark.Sql.Avro.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    [Trait("Category", "Avro")]
    public class AvroFunctionsTests
    {
        private readonly SparkSession _spark;

        public AvroFunctionsTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for Avro APIs introduced in Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            DataFrame df = _spark.Range(1);
            string jsonSchema = "{\"type\":\"long\", \"name\":\"col\"}";

            Column inputCol = df.Col("id");
            Column avroCol = ToAvro(inputCol);
            Assert.IsType<Column>(FromAvro(avroCol, jsonSchema));
        }

        /// <summary>
        /// Test signatures for Avro APIs introduced in Spark 3.0.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_0_0)]
        public void TestSignaturesV3_0_X()
        {
            DataFrame df = _spark.Range(1);
            string jsonSchema = "{\"type\":\"long\", \"name\":\"col\"}";
            var options = new Dictionary<string, string>() { { "mode", "PERMISSIVE" } };

            Column inputCol = df.Col("id");
            Column avroCol = ToAvro(inputCol, jsonSchema);
            Assert.IsType<Column>(FromAvro(avroCol, jsonSchema, options));
        }

        [Fact]
        public void RoundTripPreservesValuesAndSchema()
        {
            const string schema = "\"long\"";
            DataFrame input = _spark.Range(4);
            DataFrame encoded = input.Select(
                ToAvro(input["id"]).Alias("inferred"),
                ToAvro(input["id"], schema).Alias("explicit"));

            Assert.All(encoded.Schema().Fields,
                field => Assert.IsType<BinaryType>(field.DataType));
            Row[] encodedRows = encoded.Collect().ToArray();
            Assert.Equal(4, encodedRows.Length);
            for (int i = 0; i < encodedRows.Length; ++i)
            {
                // Avro long uses zigzag varints: 0, 1, 2, 3 encode as 00, 02, 04, 06.
                var expected = new byte[] { (byte)(i * 2) };
                Assert.Equal(expected, encodedRows[i].GetAs<byte[]>(0));
                Assert.Equal(expected, encodedRows[i].GetAs<byte[]>(1));
            }

            var options = new Dictionary<string, string> { { "mode", "FAILFAST" } };
            DataFrame decoded = encoded.Select(
                FromAvro(encoded["inferred"], schema).Alias("inferred"),
                FromAvro(encoded["explicit"], schema, options).Alias("explicit"));

            Assert.Equal(new[] { "inferred", "explicit" },
                decoded.Schema().Fields.Select(field => field.Name));
            Assert.All(decoded.Schema().Fields,
                field => Assert.IsType<LongType>(field.DataType));
            Row[] decodedRows = decoded.Collect().ToArray();
            Assert.Equal(new long[] { 0, 1, 2, 3 },
                decodedRows.Select(row => row.GetAs<long>(0)));
            Assert.Equal(new long[] { 0, 1, 2, 3 },
                decodedRows.Select(row => row.GetAs<long>(1)));
        }

        [Fact]
        public void NullableValuesRespectExplicitSchema()
        {
            const string inferredSchema = "[\"long\",\"null\"]";
            const string explicitSchema = "\"long\"";
            DataFrame input = _spark.Range(3).SelectExpr(
                "case when id = 1 then cast(null as bigint) else id end as value");
            DataFrame encoded = input.Select(
                ToAvro(input["value"]).Alias("inferred"),
                ToAvro(input["value"], explicitSchema).Alias("explicit"));

            Row[] encodedRows = encoded.Collect().ToArray();
            Assert.Equal(3, encodedRows.Length);
            Assert.Equal(new byte[] { 0, 0 }, encodedRows[0].GetAs<byte[]>(0));
            Assert.Equal(new byte[] { 0 }, encodedRows[0].GetAs<byte[]>(1));
            Assert.Null(encodedRows[1].Get(0));
            Assert.Null(encodedRows[1].Get(1));
            Assert.Equal(new byte[] { 0, 4 }, encodedRows[2].GetAs<byte[]>(0));
            Assert.Equal(new byte[] { 4 }, encodedRows[2].GetAs<byte[]>(1));

            DataFrame decoded = encoded.Select(
                FromAvro(encoded["inferred"], inferredSchema),
                FromAvro(encoded["explicit"], explicitSchema,
                    new Dictionary<string, string>()));
            Assert.All(decoded.Schema().Fields,
                field => Assert.IsType<LongType>(field.DataType));
            Row[] decodedRows = decoded.Collect().ToArray();
            Assert.Equal(3, decodedRows.Length);
            for (int column = 0; column < 2; ++column)
            {
                Assert.Equal(0L, decodedRows[0].GetAs<long>(column));
                Assert.Null(decodedRows[1].Get(column));
                Assert.Equal(2L, decodedRows[2].GetAs<long>(column));
            }
        }

        [Fact]
        public void EmptyInputRetainsAvroSchema()
        {
            DataFrame input = _spark.Range(0);
            DataFrame encoded = input.Select(ToAvro(input["id"]).Alias("value"));
            Assert.IsType<BinaryType>(encoded.Schema().Fields[0].DataType);
            Assert.Empty(encoded.Collect());

            DataFrame decoded = encoded.Select(FromAvro(encoded["value"], "\"long\""));
            Assert.IsType<LongType>(decoded.Schema().Fields[0].DataType);
            Assert.Empty(decoded.Collect());
        }

        [Fact]
        public void ReaderSchemaAddsDefaultField()
        {
            const string writerSchema = "{\"type\":\"record\",\"name\":\"Order\"," +
                "\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
            const string readerSchema = "{\"type\":\"record\",\"name\":\"Order\"," +
                "\"fields\":[{\"name\":\"id\",\"type\":\"long\"}," +
                "{\"name\":\"label\",\"type\":\"string\",\"default\":\"new\"}]}";
            var options = new Dictionary<string, string> { { "avroSchema", readerSchema } };
            DataFrame input = _spark.Range(2).SelectExpr("named_struct('id', id) as value");
            DataFrame encoded = input.Select(
                ToAvro(input["value"], writerSchema).Alias("value"));
            DataFrame decoded = encoded.Select(
                FromAvro(encoded["value"], writerSchema).Alias("original"),
                FromAvro(encoded["value"], writerSchema, options).Alias("evolved"));

            StructType originalSchema = Assert.IsType<StructType>(
                decoded.Schema().Fields[0].DataType);
            StructType evolvedSchema = Assert.IsType<StructType>(
                decoded.Schema().Fields[1].DataType);
            Assert.Equal("id", Assert.Single(originalSchema.Fields).Name);
            Assert.Equal(new[] { "id", "label" },
                evolvedSchema.Fields.Select(field => field.Name));
            Assert.IsType<LongType>(evolvedSchema.Fields[0].DataType);
            Assert.IsType<StringType>(evolvedSchema.Fields[1].DataType);

            Row[] rows = decoded.Collect().ToArray();
            Assert.Equal(2, rows.Length);
            for (int i = 0; i < rows.Length; ++i)
            {
                Assert.Equal((long)i, rows[i].GetAs<Row>(0).GetAs<long>("id"));
                Row evolved = rows[i].GetAs<Row>(1);
                Assert.Equal((long)i, evolved.GetAs<long>("id"));
                Assert.Equal("new", evolved.GetAs<string>("label"));
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void InvalidSchemaPropagatesJvmError(bool encode)
        {
            const string invalidSchema = "{not valid avro json";
            DataFrame input = _spark.Range(1);
            Exception error = Assert.ThrowsAny<Exception>(() =>
            {
                Column value = encode ?
                    ToAvro(input["id"], invalidSchema) :
                    FromAvro(ToAvro(input["id"]), invalidSchema);
                input.Select(value).Collect().ToArray();
            });

            JvmException jvmError = Assert.IsType<JvmException>(error.InnerException);
            Assert.Contains("SchemaParseException", jvmError.Message);
        }

        [Theory]
        [InlineData("")]
        [InlineData("80")]
        public void MalformedDataHonorsDecodeMode(string hex)
        {
            DataFrame input = _spark.Range(1).SelectExpr($"unhex('{hex}') as value");
            Exception error = Assert.ThrowsAny<Exception>(() =>
                input.Select(FromAvro(input["value"], "\"int\"")).Collect().ToArray());
            JvmException jvmError = Assert.IsType<JvmException>(error.InnerException);
            Assert.Contains("AvroDataToCatalyst", jvmError.Message);
            Assert.Contains("EOFException", jvmError.Message);
            Assert.Contains("FAILFAST", jvmError.Message);

            var options = new Dictionary<string, string> { { "mode", "PERMISSIVE" } };
            DataFrame permissive = input.Select(FromAvro(input["value"], "\"int\"", options));
            Assert.IsType<IntegerType>(permissive.Schema().Fields[0].DataType);
            Assert.Null(Assert.Single(permissive.Collect()).Get(0));

            DataFrame healthy = _spark.Range(1);
            Row recovered = Assert.Single(healthy.Select(
                FromAvro(ToAvro(healthy["id"]), "\"long\"")).Collect());
            Assert.Equal(0L, recovered.GetAs<long>(0));
        }
    }
}
