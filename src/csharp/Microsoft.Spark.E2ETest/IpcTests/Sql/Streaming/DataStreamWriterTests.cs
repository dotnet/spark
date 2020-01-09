// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.E2ETest.Utils;
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

        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestForeach()
        {
            // DataStreamReader will attempt to read all files in a
            // given base folder. Create a temporary folder and
            // copy our test file to it.
            using var srcTempDirectory = new TemporaryDirectory();
            File.Copy(
                $"{TestEnvironment.ResourceDirectory}people.json",
                Path.Combine(srcTempDirectory.Path, "people.json"));

            using var dstTempDirectory = new TemporaryDirectory();
            var testForeachWriter = new TestForeachWriter(dstTempDirectory.Path);

            // - Create a DataStreamReader to read all files in the srcTempDirectory.
            // - Filter entries where "age > 0"
            // - Create a DataStreamWriter and use our defined IForeachWriter to process
            //   the rows.
            DataStreamWriter dsw = _spark
                .ReadStream()
                .Format("json")
                .Schema("age INT, name STRING")
                .Load(srcTempDirectory.Path)
                .Filter("age > 0")
                .WriteStream()
                .Foreach(testForeachWriter);

            // Trigger the first stream batch
            dsw.Trigger(Trigger.Once()).Start().AwaitTermination();

            DataFrame peopleDF = _spark
                .Read()
                .Schema("age INT, name STRING")
                .Json($"{TestEnvironment.ResourceDirectory}people.json")
                .Filter("age > 0");

            DataFrame foreachWriterDF = _spark
                .Read()
                .Schema("age INT, name STRING")
                .Csv(testForeachWriter.FilePath);

            Assert.Equal(peopleDF.Collect(), foreachWriterDF.Collect());
        }

        [Serializable]
        private class TestForeachWriter : IForeachWriter
        {
            [ThreadStatic]
            private static StreamWriter _streamWriter;

            public TestForeachWriter(string writePath)
            {
                FilePath = Path.Combine(writePath, "sink-foreachWriter.csv");
            }

            public string FilePath { get; private set; }

            public void Close(Exception errorOrNull)
            {
                _streamWriter.Dispose();
            }

            public bool Open(long partitionId, long epochId)
            {
                _streamWriter = new StreamWriter(FilePath);
                return true;
            }

            public void Process(Row value)
            {
                _streamWriter.WriteLine(string.Join(",", value.Values));
            }
        }
    }
}
