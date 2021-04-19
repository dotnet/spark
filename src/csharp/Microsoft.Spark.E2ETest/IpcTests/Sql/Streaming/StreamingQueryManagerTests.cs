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
        /// Test signatures for APIs up to Spark 2.4.*.
        /// The purpose of this test is to ensure that JVM calls can be successfully made.
        /// Note that this is not testing functionality of each function.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            var intMemoryStream = new MemoryStream<int>(_spark);
            StreamingQuery sq1 = intMemoryStream
                .ToDF().WriteStream().QueryName("intQuery").Format("console").Start();

            var stringMemoryStream = new MemoryStream<string>(_spark);
            StreamingQuery sq2 = stringMemoryStream
                .ToDF().WriteStream().QueryName("stringQuery").Format("console").Start();

            StreamingQueryManager sqm = _spark.Streams();

            StreamingQuery[] streamingQueries = sqm.Active().ToArray();
            Assert.Equal(2, streamingQueries.Length);

            Assert.IsType<StreamingQuery>(sqm.Get(sq1.Id));
            Assert.IsType<StreamingQuery>(sqm.Get(sq2.Id));

            sqm.ResetTerminated();

            sqm.AwaitAnyTermination(10);

            sq1.Stop();
            sq2.Stop();
        }
    }
}
