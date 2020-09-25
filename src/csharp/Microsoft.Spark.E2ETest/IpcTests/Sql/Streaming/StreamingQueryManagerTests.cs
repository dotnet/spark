// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Linq;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class StreamingQueryManagerTests
    {
        private readonly SparkSession _spark;

        public StreamingQueryManagerTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.3.*.
        /// The purpose of this test is to ensure that JVM calls can be successfully made.
        /// Note that this is not testing functionality of each function.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            var intMemoryStream = new MemoryStream<int>(_spark);
            StreamingQuery sq1 =
                intMemoryStream.ToDF().WriteStream().QueryName("intQuery").Format("console").Start();
            string id1 = sq1.Id;

            var stringMemoryStream = new MemoryStream<string>(_spark);
            StreamingQuery sq2 =
                stringMemoryStream.ToDF().WriteStream().QueryName("stringQuery").Format("console").Start();
            string id2 = sq2.Id;

            StreamingQueryManager sqm = _spark.Streams();

            StreamingQuery[] streamingQueries = sqm.Active().ToArray();
            Assert.Equal(2, streamingQueries.Length);

            Assert.IsType<StreamingQuery>(sqm.Get(id1));
            Assert.IsType<StreamingQuery>(sqm.Get(id2));

            sqm.ResetTerminated();

            sqm.AwaitAnyTermination(1000);
        }
    }
}
