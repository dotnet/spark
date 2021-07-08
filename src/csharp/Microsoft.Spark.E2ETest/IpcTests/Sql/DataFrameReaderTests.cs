// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class DataFrameReaderTests
    {
        private readonly SparkSession _spark;

        public DataFrameReaderTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            DataFrameReader dfr = _spark.Read();

            Assert.IsType<DataFrameReader>(dfr.Format("json"));

            Assert.IsType<DataFrameReader>(
                dfr.Schema(
                    new StructType(new[]
                    {
                        new StructField("age", new IntegerType()),
                        new StructField("name", new StringType())
                    })));
            Assert.IsType<DataFrameReader>(dfr.Schema("age INT, name STRING"));

            Assert.IsType<DataFrameReader>(dfr.Option("stringOption", "value"));
            Assert.IsType<DataFrameReader>(dfr.Option("boolOption", true));
            Assert.IsType<DataFrameReader>(dfr.Option("longOption", 1L));
            Assert.IsType<DataFrameReader>(dfr.Option("doubleOption", 3D));

            Assert.IsType<DataFrameReader>(
                dfr.Options(
                    new Dictionary<string, string>
                    {
                        { "option1", "value1" },
                        { "option2", "value2" }
                    }));

            string jsonFile = $"{TestEnvironment.ResourceDirectory}people.json";
            Assert.IsType<DataFrame>(dfr.Load());
            Assert.IsType<DataFrame>(dfr.Load(jsonFile));
            Assert.IsType<DataFrame>(dfr.Load(jsonFile, jsonFile));

            Assert.IsType<DataFrame>(dfr.Json(jsonFile));
            Assert.IsType<DataFrame>(dfr.Json(jsonFile, jsonFile));

            string csvFile = $"{TestEnvironment.ResourceDirectory}people.csv";
            Assert.IsType<DataFrame>(dfr.Csv(csvFile));
            Assert.IsType<DataFrame>(dfr.Csv(csvFile, csvFile));

            string parquetFile = $"{TestEnvironment.ResourceDirectory}users.parquet";
            Assert.IsType<DataFrame>(dfr.Parquet(parquetFile));
            Assert.IsType<DataFrame>(dfr.Parquet(parquetFile, parquetFile));

            string orcFile = $"{TestEnvironment.ResourceDirectory}users.orc";
            Assert.IsType<DataFrame>(dfr.Orc(orcFile));
            Assert.IsType<DataFrame>(dfr.Orc(orcFile, orcFile));

            dfr = _spark.Read();
            string textFile = $"{TestEnvironment.ResourceDirectory}people.txt";
            Assert.IsType<DataFrame>(dfr.Text(textFile));
            Assert.IsType<DataFrame>(dfr.Text(textFile, textFile));

            _spark.Range(10).CreateOrReplaceTempView("testView");
            Assert.IsType<DataFrame>(dfr.Table("testView"));
        }
    }
}
