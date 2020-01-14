﻿// Licensed to the .NET Foundation under one or more agreements.
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

            // Test a scenario where ForeachWriter runs without issues.
            // If everything is working as expected, then:
            // - Triggering stream will not throw an exception
            // - 3 CSV files will be created in the temporary directory.
            // - 0 Exception files will be created in the temporary directory.
            // - The CSV files will contain valid data to read, where the
            //   expected entries will contain [101, 102, ..., 199]
            TestAndValidateForeach(
                streamInputPath,
                new TestForeachWriter(),
                false,
                3,
                3,
                0,
                Enumerable.Range(101, 99));

            // Test scenario where IForeachWriter.Open returns false.
            // When IForeachWriter.Open returns false, then IForeachWriter.Process
            // is not called.  Verify that:
            // - Triggering stream will not throw an exception
            // - 3 CSV files will be created in the temporary directory.
            // - 0 Exception files will be created in the temporary directory.
            // - The CSV files will not contain valid data to read.
            TestAndValidateForeach(
                streamInputPath,
                new TestForeachWriterOpenFailure(),
                false,
                3,
                3,
                0,
                Enumerable.Empty<int>());


            // Test scenario where ForeachWriter.Process throws an Exception.
            // When IForeachWriter.Process throws an Exception, then the exception
            // is rethrown by ForeachWriterWrapper. Verify that:
            // - Triggering stream throws an exception.
            // - 1 CSV file will be created in the temporary directory.
            // - 1 Exception will be created in the temporary directory. The
            //   thrown exception from Process() will be sent to Close().
            // - The CSV file will not contain valid data to read.
            TestAndValidateForeach(
                    streamInputPath,
                    new TestForeachWriterProcessFailure(),
                    true,
                    1,
                    1,
                    1,
                    Enumerable.Empty<int>());
        }

        private void TestAndValidateForeach(
            string streamInputPath,
            TestForeachWriter foreachWriter,
            bool foreachThrows,
            int partitions,
            long expectedCSVFiles,
            long expectedExceptionFiles,
            IEnumerable<int> expectedOutput)
        {
            // Temporary folder the TestForeachWriter will write to.
            using var dstTempDirectory = new TemporaryDirectory();

            foreachWriter.WritePath = dstTempDirectory.Path;

            // Read streamInputPath, repartitions data into `partitions`, then
            // calls TestForeachWriter on the data.
            DataStreamWriter dsw = _spark
                .ReadStream()
                .Schema("id INT")
                .Json(streamInputPath)
                .Repartition(partitions)
                .WriteStream()
                .Foreach(foreachWriter);

            // Trigger the stream batch once.
            if (foreachThrows)
            {
                Assert.Throws<Exception>(
                    () => dsw.Trigger(Trigger.Once()).Start().AwaitTermination());
            }
            else
            {
                dsw.Trigger(Trigger.Once()).Start().AwaitTermination();
            }

            // Verify that TestForeachWriter created a unique .csv for each
            // partition.
            Assert.Equal(
                expectedCSVFiles,
                Directory.GetFiles(dstTempDirectory.Path, "*.csv").Length);

            // When ForeachWriter.Process(Row) throws an exception,
            // ForeachWriter.Close(Exception) will create a file with the
            // .exeception extension.
            Assert.Equal(
                expectedExceptionFiles,
                Directory.GetFiles(dstTempDirectory.Path, "*.exception").Length);

            // Read in the *.csv file(s) generated by the TestForeachWriter.
            // If there are multiple input files, sorting by "id" will make
            // validation simpler.
            DataFrame foreachWriterOutputDF = _spark
                .Read()
                .Schema("id INT")
                .Csv(dstTempDirectory.Path)
                .Sort("id");

            // TestForeachWriterProcessFailure.Process(Row) did not write anything to
            // the .csv files.
            Assert.Equal(
                    expectedOutput.Select(i => new object[] { i }),
                    foreachWriterOutputDF.Collect().Select(r => r.Values));
        }

        [Serializable]
        private class TestForeachWriter : IForeachWriter
        {
            // Mark the StreamWriter as ThreadStatic. If there are multiple Tasks
            // running this ForeachWriter, on the same Worker, and in parallel, then
            // it may be possible that they may use the same StreamWriter object. This
            // may cause an unintended side effect of a Task writing the output to a
            // file meant for a different Task.
            [ThreadStatic]
            private static StreamWriter s_streamWriter;

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

                s_streamWriter?.Dispose();
            }

            public virtual bool Open(long partitionId, long epochId)
            {
                _partitionId = partitionId;
                _epochId = epochId;
                try
                {
                    s_streamWriter = new StreamWriter(
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
                s_streamWriter.WriteLine(string.Join(",", value.Values.Select(v => 100 + (int)v)));
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
