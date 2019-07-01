// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class TriggerTests
    {
        private readonly SparkSession _spark;

        public TriggerTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test StreamingQuery ProcessingTime() mode
        /// </summary>
        [Fact]
        public void TestStreamingQuery_ProcessingTime()
        {
            TestStreamingQuery("ProcessingTime");
        }

        /// <summary>
        /// Test StreamingQuery Continuous() mode
        /// </summary>
        [Fact]
        public void TestStreamingQuery_Continuous()
        {
            TestStreamingQuery("Continuous");
        }

        /// <summary>
        /// Test StreamingQuery Once() mode
        /// </summary>
        [Fact]
        public void TestStreamingQuery_Once()
        {
            TestStreamingQuery("Once");
        }

        private void TestStreamingQuery(string @case)
        {
            Trigger trigger;
            if (@case == "Once")
            {
                trigger = Trigger.Once();
            }
            else if (@case == "Continuous")
            {
                trigger = Trigger.Continuous("1 seconds");
            }
            else
            {
                trigger = Trigger.ProcessingTime(1000);
            }

            DataFrame df = _spark
                .ReadStream()
                .Format("rate")
                .Option("rowsPerSecond", 100)
                .Load();

            df = df.SelectExpr("CAST(value AS STRING)");

            StreamingQuery query = df.WriteStream()
                .Format("memory")
                .QueryName("dataTable")
                .Trigger(Trigger.Once())
                .OutputMode(OutputMode.Append)
                .Start();

            ScheduleStopQuery(query, TimeSpan.FromSeconds(5));

            query.AwaitTermination();
        }

        private static void ScheduleStopQuery(StreamingQuery query, TimeSpan time)
        {
            var _timer = new System.Timers.Timer();
            _timer.Elapsed += (o, s) =>
            {
                if (query.IsActive())
                {
                    query.Stop();
                }

                _timer.Stop();
            };
            _timer.Interval = time.TotalMilliseconds;
            _timer.Start();
        }
    }
}
