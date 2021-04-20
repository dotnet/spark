// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class StreamingQueryTests
    {
        private readonly SparkSession _spark;

        public StreamingQueryTests(SparkFixture fixture)
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
            StreamingQuery sq = intMemoryStream
                .ToDF()
                .WriteStream()
                .QueryName("testQuery")
                .Format("console")
                .Trigger(Trigger.Once())
                .Start();

            sq.AwaitTermination();
            Assert.IsType<bool>(sq.AwaitTermination(10));

            Assert.IsType<string>(sq.Name);

            Assert.IsType<string>(sq.Id);

            Assert.IsType<string>(sq.RunId);

            Assert.IsType<bool>(sq.IsActive());

            sq.Explain();

            Assert.Null(sq.Exception());

            sq.Stop();
        }
    }
}
