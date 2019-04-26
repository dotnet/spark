// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Sql;
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
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            DataFrameReader dfr = _spark.Read();

            Assert.IsType<DataFrameReader>(dfr.Format("json"));

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

            Assert.IsType<DataFrame>(dfr.Load(TestEnvironment.ResourceDirectory + "people.json"));
            Assert.IsType<DataFrame>(
                dfr.Load(
                    TestEnvironment.ResourceDirectory + "people.csv",
                    TestEnvironment.ResourceDirectory + "people.csv"));

            Assert.IsType<DataFrame>(dfr.Json(TestEnvironment.ResourceDirectory + "people.json"));
            Assert.IsType<DataFrame>(
                dfr.Json(
                    TestEnvironment.ResourceDirectory + "people.json",
                    TestEnvironment.ResourceDirectory + "people.json"));

            Assert.IsType<DataFrame>(dfr.Csv(TestEnvironment.ResourceDirectory + "people.csv"));
            Assert.IsType<DataFrame>(
                dfr.Csv(
                    TestEnvironment.ResourceDirectory + "people.csv",
                    TestEnvironment.ResourceDirectory + "people.csv"));

            Assert.IsType<DataFrame>(dfr.Parquet(TestEnvironment.ResourceDirectory + "users.parquet"));
            Assert.IsType<DataFrame>(
                dfr.Parquet(
                    TestEnvironment.ResourceDirectory + "users.parquet",
                    TestEnvironment.ResourceDirectory + "users.parquet"));

            Assert.IsType<DataFrame>(dfr.Orc(TestEnvironment.ResourceDirectory + "users.orc"));
            Assert.IsType<DataFrame>(
                dfr.Orc(
                    TestEnvironment.ResourceDirectory + "users.orc",
                    TestEnvironment.ResourceDirectory + "users.orc"));

            Assert.IsType<DataFrame>(dfr.Text(TestEnvironment.ResourceDirectory + "people.txt"));
            Assert.IsType<DataFrame>(
                dfr.Text(
                    TestEnvironment.ResourceDirectory + "people.txt",
                    TestEnvironment.ResourceDirectory + "people.txt"));
        }
    }
}
