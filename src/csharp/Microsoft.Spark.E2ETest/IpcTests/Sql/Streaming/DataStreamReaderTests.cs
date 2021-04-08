// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;
using Xunit;
using static Microsoft.Spark.E2ETest.Utils.SQLUtils;

namespace Microsoft.Spark.E2ETest.IpcTests
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
        /// Test signatures for APIs up to Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            DataStreamReader dsr = _spark.ReadStream();

            Assert.IsType<DataStreamReader>(dsr.Format("parquet"));

            Assert.IsType<DataStreamReader>(
                dsr.Schema(
                    new StructType(new[]
                    {
                        new StructField("columnName", new IntegerType())
                    })));
            Assert.IsType<DataStreamReader>(dsr.Schema("columnName bigint"));

            Assert.IsType<DataStreamReader>(dsr.Option("key", "value"));
            Assert.IsType<DataStreamReader>(dsr.Option("key", true));
            Assert.IsType<DataStreamReader>(dsr.Option("key", long.MaxValue));
            Assert.IsType<DataStreamReader>(dsr.Option("key", double.MaxValue));
            Assert.IsType<DataStreamReader>(dsr.Options(new Dictionary<string, string>()));
            Assert.IsType<DataStreamReader>(
                dsr.Options(
                    new Dictionary<string, string>
                    {
                        { "key", "value" }
                    }));

            string jsonFilePath = Path.Combine(TestEnvironment.ResourceDirectory, "people.json");
            Assert.IsType<DataFrame>(dsr.Format("json").Load(jsonFilePath));
            Assert.IsType<DataFrame>(dsr.Json(jsonFilePath));
            Assert.IsType<DataFrame>(
                dsr.Csv(Path.Combine(TestEnvironment.ResourceDirectory, "people.csv")));
            Assert.IsType<DataFrame>(
                dsr.Orc(Path.Combine(TestEnvironment.ResourceDirectory, "users.orc")));
            Assert.IsType<DataFrame>(
                dsr.Parquet(Path.Combine(TestEnvironment.ResourceDirectory, "users.parquet")));
            Assert.IsType<DataFrame>
                (dsr.Text(Path.Combine(TestEnvironment.ResourceDirectory, "people.txt")));

            // In Spark 3.1.1+ setting the `path` Option and then calling .Load(path) is not
            // supported unless `spark.sql.legacy.pathOptionBehavior.enabled` conf is set.
            // .Json(path), .Parquet(path), etc follow the same code path so the conf
            // needs to be set in these scenarios as well.
            Assert.IsType<DataFrame>(dsr.Format("json").Option("path", jsonFilePath).Load());
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.1.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_1_0)]
        public void TestSignaturesV3_1_X()
        {
            string tableName = "input_table";
            WithTable(
                _spark,
                new string[] { tableName },
                () =>
                {
                    DataStreamReader dsr = _spark.ReadStream();
                    var intMemoryStream = new MemoryStream<int>(_spark);
                    intMemoryStream.AddData(Enumerable.Range(1, 10).ToArray());
                    intMemoryStream.ToDF().CreateOrReplaceTempView(tableName);
                    Assert.IsType<DataFrame>(dsr.Table(tableName));
                });
        }
    }
}
