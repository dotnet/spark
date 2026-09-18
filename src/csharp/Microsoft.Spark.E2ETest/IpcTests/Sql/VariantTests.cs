// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Newtonsoft.Json.Linq;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    [Trait("Category", "Variant")]
    public class VariantTests
    {
        private readonly SparkSession _spark;

        public VariantTests(SparkFixture fixture) => _spark = fixture.Spark;

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void TestSqlVariantValues()
        {
            DataFrame data = _spark.Sql(
                "SELECT id, parse_json(json) AS payload FROM VALUES " +
                "(1, '{\"id\":7,\"tags\":[\"new\"]}'), (2, '[1,\"two\"]'), " +
                "(3, '12.5'), (4, '\"hello\"'), (5, 'true') AS input(id, json)");

            Assert.IsType<VariantType>(data.Schema().Fields[1].DataType);
            Assert.Equal(new[] { "id", "payload" }, data.Columns());
            Assert.Equal(new[] { Tuple.Create("id", "integer"), Tuple.Create("payload", "variant") },
                data.DTypes());

            // Keep Variant values in Spark; only ordinary strings cross into .NET.
            Row[] rows = data.Select(Col("id"), Expr("to_json(payload)").Alias("json"))
                .OrderBy("id").Collect().ToArray();
            string[] expected = { "{\"id\":7,\"tags\":[\"new\"]}", "[1,\"two\"]", "12.5", "\"hello\"", "true" };
            Assert.Equal(expected.Length, rows.Length);
            for (int i = 0; i < expected.Length; ++i)
            {
                Assert.True(JToken.DeepEquals(
                    JToken.Parse(expected[i]), JToken.Parse(rows[i].GetAs<string>("json"))));
            }

            Row extracted = data.Filter("id = 1").SelectExpr(
                "variant_get(payload, '$.id', 'int') AS id",
                "variant_get(payload, '$.tags[0]', 'string') AS tag").Collect().Single();
            Assert.Equal(7, extracted.GetAs<int>("id"));
            Assert.Equal("new", extracted.GetAs<string>("tag"));
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void TestSqlVariantNulls()
        {
            DataFrame data = _spark.Sql(
                "SELECT 1 AS id, parse_json('null') AS payload UNION ALL " +
                "SELECT 2, parse_json(CAST(NULL AS STRING)) UNION ALL " +
                "SELECT 3, variant_get(parse_json('{}'), '$.missing')");
            Assert.IsType<VariantType>(data.Schema().Fields[1].DataType);

            Row[] rows = data.SelectExpr("id", "payload IS NULL AS sql_null",
                "is_variant_null(payload) AS variant_null", "to_json(payload) AS json")
                .OrderBy("id").Collect().ToArray();
            Assert.Equal(3, rows.Length);
            Assert.False(rows[0].GetAs<bool>("sql_null"));
            Assert.True(rows[0].GetAs<bool>("variant_null"));
            Assert.Equal("null", rows[0].GetAs<string>("json"));
            foreach (Row row in rows.Skip(1))
            {
                Assert.True(row.GetAs<bool>("sql_null"));
                Assert.False(row.GetAs<bool>("variant_null"));
                Assert.Null(row.GetAs<string>("json"));
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void TestJsonReaderWithNestedVariantSchema()
        {
            var schema = new StructType(new[]
            {
                new StructField("id", new IntegerType()),
                new StructField("payload", new VariantType()),
                new StructField("nested", new StructType(new[]
                {
                    new StructField("payload", new VariantType())
                })),
                new StructField("items", new ArrayType(new VariantType())),
                new StructField("values", new MapType(new StringType(), new VariantType()))
            });

            using (var tempDirectory = new TemporaryDirectory())
            {
                string path = Path.Combine(tempDirectory.Path, "variants.json");
                File.WriteAllLines(path, new[]
                {
                    "{\"id\":1,\"payload\":null,\"nested\":{\"payload\":{\"x\":7}}," +
                        "\"items\":[1,null,{\"x\":9}],\"values\":{\"a\":11,\"n\":null}}",
                    "{\"id\":2,\"nested\":{},\"items\":[],\"values\":{}}",
                    "{\"id\":3,\"nested\":null,\"items\":null,\"values\":null}"
                });
                DataFrame data = _spark.Read().Schema(schema).Json(path);
                StructType actual = data.Schema();
                Assert.Equal(schema.Json, actual.Json);
                Assert.IsType<VariantType>(actual.Fields[1].DataType);
                Assert.IsType<VariantType>(
                    Assert.IsType<StructType>(actual.Fields[2].DataType).Fields[0].DataType);
                Assert.IsType<VariantType>(Assert.IsType<ArrayType>(actual.Fields[3].DataType).ElementType);
                Assert.IsType<VariantType>(Assert.IsType<MapType>(actual.Fields[4].DataType).ValueType);
                Assert.Equal(schema.Fields.Select(field => field.Name), data.Columns());
                Assert.Equal(schema.Fields.Select(field => Tuple.Create(field.Name, field.DataType.SimpleString)),
                    data.DTypes());

                // Filter before indexing: empty arrays must not trigger ANSI bounds errors.
                Row populated = data.Filter("id = 1").SelectExpr(
                    "payload IS NULL AS sql_null", "is_variant_null(payload) AS variant_null",
                    "variant_get(nested.payload, '$.x', 'int') AS nested_value",
                    "variant_get(items[0], '$', 'int') AS first_item",
                    "variant_get(items[2], '$.x', 'int') AS last_item",
                    "variant_get(values['a'], '$', 'int') AS map_value",
                    "items[1] IS NULL AS item_sql_null", "is_variant_null(items[1]) AS item_variant_null",
                    "values['n'] IS NULL AS map_sql_null", "is_variant_null(values['n']) AS map_variant_null")
                    .Collect().Single();
                Assert.False(populated.GetAs<bool>("sql_null"));
                Assert.True(populated.GetAs<bool>("variant_null"));
                Assert.Equal(7, populated.GetAs<int>("nested_value"));
                Assert.Equal(1, populated.GetAs<int>("first_item"));
                Assert.Equal(9, populated.GetAs<int>("last_item"));
                Assert.Equal(11, populated.GetAs<int>("map_value"));
                Assert.False(populated.GetAs<bool>("item_sql_null"));
                Assert.True(populated.GetAs<bool>("item_variant_null"));
                Assert.False(populated.GetAs<bool>("map_sql_null"));
                Assert.True(populated.GetAs<bool>("map_variant_null"));

                Row missing = data.Filter("id = 2").SelectExpr(
                    "payload IS NULL", "nested.payload IS NULL", "nested IS NOT NULL",
                    "size(items) = 0", "size(values) = 0", "values['missing'] IS NULL")
                    .Collect().Single();
                Assert.All(missing.Values, value => Assert.Equal(true, value));

                Row nulls = data.Filter("id = 3").SelectExpr(
                    "payload IS NULL", "nested IS NULL", "items IS NULL", "values IS NULL")
                    .Collect().Single();
                Assert.All(nulls.Values, value => Assert.Equal(true, value));
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void TestVariantConversionFailureAndRecovery()
        {
            Exception failure = Assert.Throws<Exception>(() => _spark.Sql(
                "SELECT variant_get(parse_json('{\"id\":\"not-an-int\"}'), '$.id', 'int')")
                .Collect().ToArray());
            Assert.IsType<JvmException>(failure.InnerException);
            Assert.Contains("INVALID_VARIANT_CAST", failure.InnerException.Message);

            Row recovered = _spark.Sql(
                "SELECT variant_get(parse_json('{\"id\":7}'), '$.id', 'int') AS id, " +
                "try_variant_get(parse_json('{\"id\":\"not-an-int\"}'), '$.id', 'int') AS invalid, " +
                "variant_get(parse_json('{}'), '$.missing', 'int') AS missing")
                .Collect().Single();
            Assert.Equal(7, recovered.GetAs<int>("id"));
            Assert.Null(recovered[1]);
            Assert.Null(recovered[2]);
        }
    }
}
