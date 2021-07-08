// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;
using static Microsoft.Spark.E2ETest.Utils.SQLUtils;
using static Microsoft.Spark.Sql.Functions;

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
        /// Test signatures for APIs up to Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
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

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.1.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_1_0)]
        public void TestSignaturesV3_1_X()
        {
            string tableName = "output_table";
            WithTable(
                _spark,
                new string[] { tableName },
                () =>
                {
                    using var tempDirectory = new TemporaryDirectory();
                    var intMemoryStream = new MemoryStream<int>(_spark);
                    DataStreamWriter dsw = intMemoryStream
                        .ToDF()
                        .WriteStream()
                        .Format("parquet")
                        .Option("checkpointLocation", tempDirectory.Path);

                    StreamingQuery sq = dsw.ToTable(tableName);
                    sq.Stop();
                });
        }

        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestForeachBatch()
        {
            // Temporary folder to put our test stream input.
            using var srcTempDirectory = new TemporaryDirectory();
            // Temporary folder to write ForeachBatch output.
            using var dstTempDirectory = new TemporaryDirectory();

            Func<Column, Column> outerUdf = Udf<int, int>(i => i + 100);

            // id column: [0, 1, ..., 9]
            WriteCsv(0, 10, Path.Combine(srcTempDirectory.Path, "input1.csv"));

            DataStreamWriter dsw = _spark
                .ReadStream()
                .Schema("id INT")
                .Csv(srcTempDirectory.Path)
                .WriteStream()
                .ForeachBatch((df, id) =>
                {
                    Func<Column, Column> innerUdf = Udf<int, int>(i => i + 200);
                    df.Select(outerUdf(innerUdf(Col("id"))))
                        .Write()
                        .Csv(Path.Combine(dstTempDirectory.Path, id.ToString()));
                });

            StreamingQuery sq = dsw.Start();

            // Process until all available data in the source has been processed and committed
            // to the ForeachBatch sink. 
            sq.ProcessAllAvailable();

            // Add new file to the source path. The spark stream will read any new files
            // added to the source path.
            // id column: [10, 11, ..., 19]
            WriteCsv(10, 10, Path.Combine(srcTempDirectory.Path, "input2.csv"));

            // Process until all available data in the source has been processed and committed
            // to the ForeachBatch sink.
            sq.ProcessAllAvailable();
            sq.Stop();

            // Verify folders in the destination path.
            string[] csvPaths =
                Directory.GetDirectories(dstTempDirectory.Path).OrderBy(s => s).ToArray();
            var expectedPaths = new string[]
            {
                Path.Combine(dstTempDirectory.Path, "0"),
                Path.Combine(dstTempDirectory.Path, "1"),
            };
            Assert.True(expectedPaths.SequenceEqual(csvPaths));

            // Read the generated csv paths and verify contents.
            DataFrame df = _spark
                .Read()
                .Schema("id INT")
                .Csv(csvPaths[0], csvPaths[1])
                .Sort("id");

            IEnumerable<int> actualIds = df.Collect().Select(r => r.GetAs<int>("id"));
            Assert.True(Enumerable.Range(300, 20).SequenceEqual(actualIds));
        }

        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestForeach()
        {
            // Temporary folder to put our test stream input.
            using var srcTempDirectory = new TemporaryDirectory();
            string streamInputPath = Path.Combine(srcTempDirectory.Path, "streamInput");

            Func<Column, Column> intToStrUdf = Udf<int, string>(i => i.ToString());

            // id column: [1, 2, ..., 99]
            // idStr column: "id" column converted to string
            // idAndIdStr column: Struct column composed from the "id" and "idStr" column.
            _spark.Range(1, 100)
                .WithColumn("idStr", intToStrUdf(Col("id")))
                .WithColumn("idAndIdStr", Struct("id", "idStr"))
                .Write()
                .Json(streamInputPath);

            // Test a scenario where IForeachWriter runs without issues.
            // If everything is working as expected, then:
            // - Triggering stream will not throw an exception
            // - 3 CSV files will be created in the temporary directory.
            // - 0 Exception files will be created in the temporary directory.
            // - The CSV files will contain valid data to read, where the
            //   expected entries will contain [1111, 2222, ..., 99999999]
            TestAndValidateForeach(
                streamInputPath,
                new TestForeachWriter(),
                3,
                0,
                Enumerable.Range(1, 99).Select(i => Convert.ToInt32($"{i}{i}{i}{i}")));

            // Test scenario where IForeachWriter.Open returns false.
            // When IForeachWriter.Open returns false, then IForeachWriter.Process
            // is not called. Verify that:
            // - Triggering stream will not throw an exception
            // - 3 CSV files will be created in the temporary directory.
            // - 0 Exception files will be created in the temporary directory.
            // - The CSV files will not contain valid data to read.
            TestAndValidateForeach(
                streamInputPath,
                new TestForeachWriterOpenFailure(),
                3,
                0,
                Enumerable.Empty<int>());

            // Test scenario where IForeachWriter.Process throws an Exception.
            // When IForeachWriter.Process throws an Exception, then the exception
            // is rethrown by ForeachWriterWrapper. We will limit the partitions
            // to 1 to make validating this scenario simpler. Verify that:
            // - Triggering stream throws an exception.
            // - 1 CSV file will be created in the temporary directory.
            // - 1 Exception will be created in the temporary directory. The
            //   thrown exception from Process() will be sent to Close().
            // - The CSV file will not contain valid data to read.
            TestAndValidateForeach(
                streamInputPath,
                new TestForeachWriterProcessFailure(),
                1,
                1,
                Enumerable.Empty<int>());
        }

        private void TestAndValidateForeach(
            string streamInputPath,
            TestForeachWriter foreachWriter,
            int expectedCSVFiles,
            int expectedExceptionFiles,
            IEnumerable<int> expectedOutput)
        {
            // Temporary folder the TestForeachWriter will write to.
            using var dstTempDirectory = new TemporaryDirectory();
            foreachWriter.WritePath = dstTempDirectory.Path;

            // Read streamInputPath, repartition data, then
            // call TestForeachWriter on the data.
            DataStreamWriter dsw = _spark
                .ReadStream()
                .Schema(new StructType(new[]
                {
                    new StructField("id", new IntegerType()),
                    new StructField("idStr", new StringType()),
                    new StructField("idAndIdStr", new StructType(new[]
                    {
                        new StructField("id", new IntegerType()),
                        new StructField("idStr", new StringType())
                    }))
                }))
                .Json(streamInputPath)
                .Repartition(expectedCSVFiles)
                .WriteStream()
                .Foreach(foreachWriter);

            // Trigger the stream batch once.
            if (expectedExceptionFiles > 0)
            {
                Assert.Throws<Exception>(
                    () => dsw.Trigger(Trigger.Once()).Start().AwaitTermination());
            }
            else
            {
                dsw.Trigger(Trigger.Once()).Start().AwaitTermination();
            }

            // Verify that TestForeachWriter created a unique .csv when
            // ForeachWriter.Open was called on each partitionId.
            Assert.Equal(
                expectedCSVFiles,
                Directory.GetFiles(dstTempDirectory.Path, "*.csv").Length);

            // Only if ForeachWriter.Process(Row) throws an exception, will
            // ForeachWriter.Close(Exception) create a file with the
            // .exception extension.
            Assert.Equal(
                expectedExceptionFiles,
                Directory.GetFiles(dstTempDirectory.Path, "*.exception").Length);

            // Read in the *.csv file(s) generated by the TestForeachWriter.
            // If there are multiple input files, sorting by "id" will make
            // validation simpler. Contents of the *.csv will only be populated
            // on successful calls to the ForeachWriter.Process method.
            DataFrame foreachWriterOutputDF = _spark
                .Read()
                .Schema("id INT")
                .Csv(dstTempDirectory.Path)
                .Sort("id");

            // Validate expected *.csv data.
            Assert.Equal(
                expectedOutput.Select(i => new object[] { i }),
                foreachWriterOutputDF.Collect().Select(r => r.Values));
        }

        private void WriteCsv(int start, int count, string path)
        {
            using var streamWriter = new StreamWriter(path);
            foreach (int i in Enumerable.Range(start, count))
            {
                streamWriter.WriteLine(i);
            }
        }

        [Serializable]
        private class TestForeachWriter : IForeachWriter
        {
            [NonSerialized]
            private StreamWriter _streamWriter;

            private long _partitionId;

            private long _epochId;

            internal string WritePath { get; set; }

            public void Close(Exception errorOrNull)
            {
                if (errorOrNull != null)
                {
                    FileStream fs = File.Create(
                        Path.Combine(
                            WritePath,
                            $"Close-{_partitionId}-{_epochId}.exception"));
                    fs.Dispose();
                }

                _streamWriter?.Dispose();
            }

            public virtual bool Open(long partitionId, long epochId)
            {
                _partitionId = partitionId;
                _epochId = epochId;
                try
                {
                    _streamWriter = new StreamWriter(
                        Path.Combine(
                            WritePath,
                            $"sink-foreachWriter-{_partitionId}-{_epochId}.csv"));
                    return true;
                }
                catch
                {
                    return false;
                }
            }

            public virtual void Process(Row value)
            {
                Row idAndIdStr = value.GetAs<Row>("idAndIdStr");
                _streamWriter.WriteLine(
                    string.Format("{0}{1}{2}{3}",
                        value.GetAs<int>("id"),
                        value.GetAs<string>("idStr"),
                        idAndIdStr.GetAs<int>("id"),
                        idAndIdStr.GetAs<string>("idStr")));
            }
        }

        [Serializable]
        private class TestForeachWriterOpenFailure : TestForeachWriter
        {
            public override bool Open(long partitionId, long epochId)
            {
                base.Open(partitionId, epochId);
                return false;
            }
        }

        [Serializable]
        private class TestForeachWriterProcessFailure : TestForeachWriter
        {
            public override void Process(Row value)
            {
                throw new Exception("TestForeachWriterProcessFailure Process(Row) failure.");
            }
        }
    }
}
