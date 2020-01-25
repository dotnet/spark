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
            // Temporary folder to put our test stream input.
            using var srcTempDirectory = new TemporaryDirectory();
            string streamInputPath = Path.Combine(srcTempDirectory.Path, "streamInput");

            // [1, 2, ..., 99]
            _spark.Range(1, 100).Write().Json(streamInputPath);

            // Test a scenario where IForeachWriter runs without issues.
            // If everything is working as expected, then:
            // - Triggering stream will not throw an exception
            // - 3 CSV files will be created in the temporary directory.
            // - 0 Exception files will be created in the temporary directory.
            // - The CSV files will contain valid data to read, where the
            //   expected entries will contain [101, 102, ..., 199]
            TestAndValidateForeach(
                streamInputPath,
                new TestForeachWriter(),
                3,
                0,
                Enumerable.Range(101, 99));

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
                .Schema("id INT")
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
            // .exeception extension.
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

            // Validated expected *.csv data.
            Assert.Equal(
                expectedOutput.Select(i => new object[] { i }),
                foreachWriterOutputDF.Collect().Select(r => r.Values));
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
                _streamWriter.WriteLine(string.Join(",", value.Values.Select(v => 100 + (int)v)));
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
