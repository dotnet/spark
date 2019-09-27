// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class DataStreamWriterTests
    {
        private readonly SparkSession _spark;

        public DataStreamWriterTests(SparkFixture fixture)
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

            DataStreamWriter dsw = df.WriteStream();

            Assert.IsType<DataStreamWriter>(dsw.OutputMode("append"));

            Assert.IsType<DataStreamWriter>(dsw.OutputMode(OutputMode.Append));

            Assert.IsType<DataStreamWriter>(dsw.Format("json"));

            Assert.IsType<DataStreamWriter>(dsw.Option("stringOption", "value"));
            Assert.IsType<DataStreamWriter>(dsw.Option("boolOption", true));
            Assert.IsType<DataStreamWriter>(dsw.Option("longOption", 1L));
            Assert.IsType<DataStreamWriter>(dsw.Option("doubleOption", 3D));

            Assert.IsType<DataStreamWriter>(
                dsw.Options(
                    new Dictionary<string, string>
                    {
                        { "option1", "value1" },
                        { "option2", "value2" }
                    }));

            Assert.IsType<DataStreamWriter>(dsw.PartitionBy("age"));
            Assert.IsType<DataStreamWriter>(dsw.PartitionBy("age", "name"));

            Assert.IsType<DataStreamWriter>(dsw.QueryName("queryName"));

            Assert.IsType<DataStreamWriter>(dsw.Trigger(Trigger.Once()));
        }
    }
}
