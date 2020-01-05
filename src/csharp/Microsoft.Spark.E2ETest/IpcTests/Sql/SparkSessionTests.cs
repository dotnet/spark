// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
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
        /// Test CreateDataFrame APIs.
        /// </summary>        
        [Fact]
        public void TestCreateDataFrame()
        {            
            // Calling CreateDataFrame with schema
            {
                var data = new List<GenericRow>();
                data.Add(new GenericRow(new object[] { "Alice", 20 }));
                data.Add(new GenericRow(new object[] { "Bob", 30 }));

                var schema = new StructType(new List<StructField>()
                {
                    new StructField("Name", new StringType()),
                    new StructField("Age", new IntegerType())
                });
                DataFrame df = _spark.CreateDataFrame(data, schema);                
                ValidateDataFrame<object[]>(data.ConvertAll(a => a.Values), df, schema);
            }

            // Calling CreateDataFrame(IEnumerable<string> _) without schema
            {
                var data = new List<string>(new string[] { "Alice", "Bob" });
                var schema = new StructType(new List<StructField>()
                {
                    new StructField("_1", new StringType())
                });

                DataFrame df = _spark.CreateDataFrame(data);                
                ValidateDataFrame<object[]>(data.ConvertAll(a => new object[] { a }), df, schema);
            }

            // Calling CreateDataFrame(IEnumerable<int> _) without schema
            {
                var data = new List<int>(new int[] { 1, 2 });
                var schema = new StructType(new List<StructField>()
                {
                    new StructField("_1", new IntegerType())
                });

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame<object[]>(data.ConvertAll(a => new object[] { a }), df, schema);
            }

            // Calling CreateDataFrame(IEnumerable<double> _) without schema
            {
                var data = new List<double>(new double[] { 1.2, 2.3 });
                var schema = new StructType(new List<StructField>()
                {
                    new StructField("_1", new DoubleType())
                });

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame<object[]>(data.ConvertAll(a => new object[] { a }), df, schema);
            }

            // Calling CreateDataFrame(IEnumerable<bool> _) without schema
            {
                var data = new List<bool>(new bool[] { true, false });
                var schema = new StructType(new List<StructField>()
                {
                    new StructField("_1", new BooleanType())
                });

                DataFrame df = _spark.CreateDataFrame(data);
                ValidateDataFrame<object[]>(data.ConvertAll(a => new object[] { a }), df, schema);
            }
        }

        /// <summary>
        /// Validates that the dataframe returned by the CreateDataFrame API is correct as per the
        /// given data.
        /// </summary>
        /// <param name="data">Given data for the dataframe</param>
        /// <param name="df">Dataframe Object to validate</param>
        /// <param name="schema">Expected schema of the dataframe</param>
        internal void ValidateDataFrame<T>(List<object[]> data, DataFrame df, StructType schema)
        {
            Assert.Equal(schema, df.Schema());
            Row[] rows = df.Collect().ToArray();
            Assert.Equal(data.Count, rows.Length);
            for (int i = 0; i < rows.Length; ++i)
            {
                Assert.Equal(data[i], rows[i].Values);
            }
        }
    }
}
