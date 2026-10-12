// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Catalog;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    [Trait("Category", "DriverApi")]
    public class DriverApiCompatibilityTests
    {
        private readonly SparkSession _spark;

        public DriverApiCompatibilityTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestSqlDataFrameParquetRoundTrip()
        {
            DataFrame orders = _spark.Sql(@"
                SELECT * FROM VALUES
                    (1, 'w', 2, 10),
                    (2, 'w', 3, 7),
                    (3, 'e', 4, 5),
                    (4, 'e', 1, 8),
                    (5, 'x', 5, 6)
                AS orders(order_id, region_id, quantity, unit_price)");
            DataFrame regions = _spark.Sql(@"
                SELECT * FROM VALUES ('w', 'West'), ('e', 'East')
                AS regions(region_id, region)");

            DataFrame totals = orders
                .Select(
                    Col("region_id"),
                    (Col("quantity") * Col("unit_price")).Alias("amount"))
                .Filter(Col("amount") >= 20)
                .Join(regions, "region_id")
                .GroupBy("region")
                .Agg(Sum("amount").Alias("total"), Count(Lit(1)).Alias("order_count"));

            AssertOrderTotals(totals);
            Assert.Equal(new[] { "region", "total", "order_count" }, totals.Columns());
            Assert.IsType<StringType>(totals.Schema().Fields[0].DataType);
            Assert.IsType<LongType>(totals.Schema().Fields[1].DataType);
            Assert.IsType<LongType>(totals.Schema().Fields[2].DataType);

            using (var directory = new TemporaryDirectory())
            {
                string path = Path.Combine(directory.Path, "orders");
                totals.Write().Parquet(path);
                DataFrame restored = _spark.Read().Parquet(path);

                // Parquet reads make fields nullable even when their input was non-nullable.
                var expectedSchema = new StructType(new[]
                {
                    new StructField("region", new StringType()),
                    new StructField("total", new LongType()),
                    new StructField("order_count", new LongType())
                });
                Assert.Equal(expectedSchema, restored.Schema());
                AssertOrderTotals(restored);
            }
        }

        [Fact]
        public void TestCatalogViewAndColumnResults()
        {
            string viewName = "driver_api_" + Guid.NewGuid().ToString("N");
            Catalog catalog = _spark.Catalog;
            DataFrame result = _spark.Sql(
                "SELECT 3 AS quantity, 7 AS unit_price, 'spark' AS product")
                .Select(
                    (Col("quantity") * Col("unit_price")).Alias("amount"),
                    Upper(Col("product")).Alias("name"),
                    (Col("quantity") >= 2).Alias("bulk"),
                    Col("quantity").Cast("string").Alias("quantity_text"));

            try
            {
                result.CreateOrReplaceTempView(viewName);
                Assert.True(catalog.TableExists(viewName));

                Table table = catalog.GetTable(viewName);
                Assert.Equal(viewName, table.Name);
                Assert.True(table.IsTemporary);
                Assert.Equal("TEMPORARY", table.TableType);
                Assert.Null(table.Database);
                Assert.Null(table.Description);

                Row listedTable = Assert.Single(catalog.ListTables()
                    .Filter(Col("name") == viewName)
                    .Select("name", "tableType", "isTemporary")
                    .Collect());
                Assert.Equal(new object[] { viewName, "TEMPORARY", true }, listedTable.Values);

                var expectedColumns = new[]
                {
                    new object[] { "amount", "int", false, false },
                    new object[] { "bulk", "boolean", false, false },
                    new object[] { "name", "string", false, false },
                    new object[] { "quantity_text", "string", false, false }
                };
                Assert.Equal(
                    expectedColumns,
                    catalog.ListColumns(viewName)
                        .Select("name", "dataType", "isPartition", "isBucket")
                        .OrderBy("name")
                        .Collect()
                        .Select(row => row.Values));

                Row row = Assert.Single(_spark.Table(viewName).Collect());
                Assert.Equal(new object[] { 21, "SPARK", true, "3" }, row.Values);
            }
            finally
            {
                catalog.DropTempView(viewName);
            }

            Assert.False(catalog.TableExists(viewName));
        }

        [Fact]
        public void TestAnsiCastFailureAndRecovery()
        {
            const string ansiKey = "spark.sql.ansi.enabled";
            const string invalidCast = "SELECT CAST('not-an-integer' AS INT) AS value";
            SparkSession session = _spark.NewSession();
            RuntimeConfig conf = session.Conf();
            string originalAnsi = conf.Get(ansiKey);
            string sharedAnsi = _spark.Conf().Get(ansiKey);

            try
            {
                conf.Set(ansiKey, true);
                Exception exception = Assert.Throws<Exception>(() =>
                    session.Sql(invalidCast).Collect().ToArray());
                JvmException jvmException = Assert.IsType<JvmException>(exception.InnerException);
                Assert.Contains("NumberFormatException", jvmException.Message);
                Assert.Contains("not-an-integer", jvmException.Message);
                Assert.Equal(42, session.Sql("SELECT 40 + 2 AS value").First().GetAs<int>(0));

                conf.Set(ansiKey, false);
                Row permissiveRow = Assert.Single(session.Sql(invalidCast).Collect());
                Assert.Null(permissiveRow.Values[0]);
            }
            finally
            {
                conf.Set(ansiKey, originalAnsi);
            }

            Assert.Equal(originalAnsi, conf.Get(ansiKey));
            Assert.Equal(sharedAnsi, _spark.Conf().Get(ansiKey));
            Assert.Equal(42, session.Sql("SELECT 40 + 2 AS value").First().GetAs<int>(0));
        }

        private static void AssertOrderTotals(DataFrame totals)
        {
            // GetAs handles small BIGINT values that arrive boxed as Int32 through Pickle.
            Assert.Collection(
                totals.OrderBy("region").Collect(),
                row =>
                {
                    Assert.Equal("East", row.GetAs<string>("region"));
                    Assert.Equal(20L, row.GetAs<long>("total"));
                    Assert.Equal(1L, row.GetAs<long>("order_count"));
                },
                row =>
                {
                    Assert.Equal("West", row.GetAs<string>("region"));
                    Assert.Equal(41L, row.GetAs<long>("total"));
                    Assert.Equal(2L, row.GetAs<long>("order_count"));
                });
        }
    }
}
