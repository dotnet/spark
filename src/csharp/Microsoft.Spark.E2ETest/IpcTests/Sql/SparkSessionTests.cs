// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.E2ETest.Utils;
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
        /// Test signatures for APIs up to Spark 2.3.*.
        /// The purpose of this test is to ensure that JVM calls can be successfully made.
        /// Note that this is not testing functionality of each function.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            Assert.IsType<SparkContext>(_spark.SparkContext);

            Assert.IsType<Builder>(SparkSession.Builder());

            SparkSession.ClearDefaultSession();
            SparkSession.SetDefaultSession(_spark);
            Assert.IsType<SparkSession>(SparkSession.GetDefaultSession());

            Assert.IsType<RuntimeConfig>(_spark.Conf());

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

            Assert.IsType<Catalog>(_spark.Catalog());
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 2.4.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestSignaturesV2_4_X()
        {
            Assert.IsType<SparkSession>(SparkSession.Active());
        }

        /// <summary>
        /// Test CreateDataFrame API.
        /// </summary>        
        [Fact]
        public void TestCreateDataFrame()
        {                                   
            var schema = new StructType(new List<StructField>()
            {
                new StructField("Name1", new StringType()),
                new StructField("Name2", new StringType())
            });

            List<GenericRow> data = new List<GenericRow>();
            data.Add(new GenericRow(new object[] { "Alice", "Harry" }));
            data.Add(new GenericRow(new object[] { "Bob", "Mary" }));

            // Calling CreateDataFrame with schema
            DataFrame df1 = _spark.CreateDataFrame(data, schema);
            Assert.IsType<DataFrame>(df1);
            Assert.Equal<StructType>(schema, df1.Schema());
            var df1Collect = df1.Collect();
            int numRow = 0;
            foreach (Row r in df1Collect)
            {
                // Checking the values from the dataFrame are same as the values in given data
                Assert.Equal<object>(r.Values, data[numRow].Values);
                ++numRow;
            }
            Assert.Equal<int>(2, numRow);

            // Calling CreateDataFrame without schema
            DataFrame df2 = _spark.CreateDataFrame(data);            
            var expectedSchema = new StructType(new List<StructField>()
            {
                new StructField("_1", new StringType()),
                new StructField("_2", new StringType())
            });
            Assert.IsType<DataFrame>(df2);
            Assert.Equal<StructType>(expectedSchema, df2.Schema());
            var df2Collect = df2.Collect();
            numRow = 0;
            foreach (Row r in df2Collect)
            {
                // Checking the values from the dataFrame are same as the values in given data
                Assert.Equal<object>(r.Values, data[numRow].Values);
                ++numRow;
            }
            Assert.Equal<int>(2, numRow);
        }
    }
}
