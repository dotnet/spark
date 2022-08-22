// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Catalog;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class SparkSessionTests
    {
        private readonly SparkSession _spark;

        public SparkSessionTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.4.*.
        /// The purpose of this test is to ensure that JVM calls can be successfully made.
        /// Note that this is not testing functionality of each function.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            Assert.IsType<SparkContext>(_spark.SparkContext);

            Assert.IsType<Builder>(SparkSession.Builder());

            SparkSession.ClearActiveSession();
            SparkSession.SetActiveSession(_spark);
            Assert.IsType<SparkSession>(SparkSession.GetActiveSession());

            SparkSession.ClearDefaultSession();
            SparkSession.SetDefaultSession(_spark);
            Assert.IsType<SparkSession>(SparkSession.GetDefaultSession());

            Assert.IsType<RuntimeConfig>(_spark.Conf());

            Assert.IsType<StreamingQueryManager>(_spark.Streams());

            Assert.IsType<SparkSession>(_spark.NewSession());

            Assert.IsType<DataFrameReader>(_spark.Read());

            Assert.IsType<DataFrame>(_spark.Range(10));
            Assert.IsType<DataFrame>(_spark.Range(10, 100));
            Assert.IsType<DataFrame>(_spark.Range(10, 100, 10));
            Assert.IsType<DataFrame>(_spark.Range(10, 100, 10, 5));

            _spark.Range(10).CreateOrReplaceTempView("testView");
            Assert.IsType<DataFrame>(_spark.Table("testView"));

            Assert.IsType<DataStreamReader>(_spark.ReadStream());

            Assert.IsType<UdfRegistration>(_spark.Udf());

            Assert.IsType<Catalog>(_spark.Catalog);

            Assert.NotNull(_spark.Version());
        
            Assert.IsType<SparkSession>(SparkSession.Active());
        }

        /// <summary>
        /// Test CreateDataFrame APIs.
        /// </summary>
        [Fact]
        public void TestCreateDataFrame()
        {
            // Calling CreateDataFrame with schema
            {
                var data = new List<GenericRow>
                {
                    new GenericRow(new object[] { "Alice", 20, new Date(2020, 1, 1) }),
                    new GenericRow(new object[] { "Bob", 30, new Date(2020, 1, 2) })
                };

                var schema = new StructType(new List<StructField>()
                {
                    new StructField("Name", new StringType()),
                    new StructField("Age", new IntegerType()),
                    new StructField("Date", new DateType())
                });
                DataFrame df = _spark.CreateDataFrame(data, schema);
                ValidateDataFrame(df, data.Select(a => a.Values), schema);
            }

            // Calling CreateDataFrame(IEnumerable<string> _) without schema
            {
                var data = new string[] { "Alice", "Bob", null };
                StructType schema = SchemaWithSingleColumn(new StringType());

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame(df, data.Select(a => new object[] { a }), schema);
            }

            // Calling CreateDataFrame(IEnumerable<int> _) without schema
            {
                var data = new int[] { 1, 2 };
                StructType schema = SchemaWithSingleColumn(new IntegerType(), false);

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame(df, data.Select(a => new object[] { a }), schema);
            }

            // Calling CreateDataFrame(IEnumerable<int?> _) without schema
            {
                var data = new int?[] { 1, 2, null };
                StructType schema = SchemaWithSingleColumn(new IntegerType());

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame(df, data.Select(a => new object[] { a }), schema);
            }

            // Calling CreateDataFrame(IEnumerable<double> _) without schema
            {
                var data = new double[] { 1.2, 2.3 };
                StructType schema = SchemaWithSingleColumn(new DoubleType(), false);

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame(df, data.Select(a => new object[] { a }), schema);
            }

            // Calling CreateDataFrame(IEnumerable<double?> _) without schema
            {
                var data = new double?[] { 1.2, 2.3, null };
                StructType schema = SchemaWithSingleColumn(new DoubleType());

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame(df, data.Select(a => new object[] { a }), schema);
            }

            // Calling CreateDataFrame(IEnumerable<bool> _) without schema
            {
                var data = new bool[] { true, false };
                StructType schema = SchemaWithSingleColumn(new BooleanType(), false);

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame(df, data.Select(a => new object[] { a }), schema);
            }

            // Calling CreateDataFrame(IEnumerable<bool?> _) without schema
            {
                var data = new bool?[] { true, false, null };
                StructType schema = SchemaWithSingleColumn(new BooleanType());

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame(df, data.Select(a => new object[] { a }), schema);
            }

            // Calling CreateDataFrame(IEnumerable<Date> _) without schema
            {
                var data = new Date[]
                {
                    new Date(2020, 1, 1),
                    new Date(2020, 1, 2),
                    null
                };
                StructType schema = SchemaWithSingleColumn(new DateType());

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame(df, data.Select(a => new object[] { a }), schema);
            }
        }

        /// <summary>
        /// Test CreateDataFrame API with Timestamp as data
        /// </summary>
        [Fact]
        public void TestCreateDataFrameWithTimestamp()
        {
            var data = new Timestamp[]
                {
                    new Timestamp(2020, 1, 1, 0, 0, 0, 0),
                    new Timestamp(2020, 1, 2, 15, 30, 30, 0),
                    null
                };
            StructType schema = SchemaWithSingleColumn(new TimestampType());

            DataFrame df = _spark.CreateDataFrame(data);
            ValidateDataFrame(df, data.Select(a => new object[] { a }), schema);
        }

        private void ValidateDataFrame(
            DataFrame actual,
            IEnumerable<object[]> expectedRows,
            StructType expectedSchema)
        {
            Assert.Equal(expectedSchema, actual.Schema());
            Assert.Equal(expectedRows, actual.Collect().Select(r => r.Values));
        }

        /// <summary>
        /// Returns a single column schema of the given datatype.
        /// </summary>
        /// <param name="dataType">Datatype of the column</param>
        /// <param name="isNullable">Indicates if values of the column can be null</param>
        /// <returns>Schema as StructType</returns>
        private StructType SchemaWithSingleColumn(DataType dataType, bool isNullable = true) =>
            new StructType(new[] { new StructField("_1", dataType, isNullable) });
    }
}
