// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

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
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        [Fact]
        public void TestStreamingV2_3_X()
        {
            DataFrame df = _spark
                .ReadStream()
                .Schema("age INT, name STRING")
                .Json($"{TestEnvironment.ResourceDirectory}people.json");

            DataFrame result = df.Select(
                df.Col("name"),
                Concat(df.Col("name"), df.Col("age")).As("text"));

            StreamingQuery query = result.WriteStream()
                .Format("memory")
                .QueryName("dataTable")
                .Trigger(Trigger.Continuous("1 seconds"))
                .Start();

            query.AwaitTermination();


            _spark.Sql("SELECT * FROM dataTable").Show();

        }
    }
}
