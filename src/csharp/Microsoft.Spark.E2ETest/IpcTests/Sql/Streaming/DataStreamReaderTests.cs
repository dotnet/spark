// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.Sql.Streaming
{
    [Collection("Spark E2E Tests")]
    public class DataStreamReaderTests
    {
        private readonly SparkSession _spark;

        public DataStreamReaderTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            DataFrame df = _spark
                .ReadStream()
                .Format("rate")
                .Option("rowsPerSecond", 1)
                .Load();

            DataStreamReader dsr = _spark.ReadStream();

            Assert.IsType<DataStreamReader>(dsr.Format("source"));
            Assert.IsType<DataStreamReader>(dsr.Schema("schematring"));
            Assert.IsType<DataStreamReader>(dsr.Option("key", "value"));
            Assert.IsType<DataStreamReader>(dsr.Option("key", false));
            Assert.IsType<DataStreamReader>(dsr.Option("key", long.MaxValue));
            Assert.IsType<DataStreamReader>(dsr.Option("key", double.MaxValue));
            Assert.IsType<DataStreamReader>(dsr.Options(new Dictionary<string, string>()));

            Assert.IsType<DataFrame>(dsr.Load());
            Assert.IsType<DataFrame>(dsr.Load("path"));
            Assert.IsType<DataFrame>(dsr.Json("path"));
            Assert.IsType<DataFrame>(dsr.Csv("path"));
            Assert.IsType<DataFrame>(dsr.Orc("path"));
            Assert.IsType<DataFrame>(dsr.Parquet("path"));
            Assert.IsType<DataFrame>(dsr.Text("path"));
        }
    }
}
