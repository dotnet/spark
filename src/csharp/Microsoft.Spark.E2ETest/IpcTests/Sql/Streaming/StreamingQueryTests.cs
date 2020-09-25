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
    public class StreamingQueryTests
    {
        private readonly SparkSession _spark;

        public StreamingQueryTests(SparkFixture fixture)
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
            StreamingQuery sq = intMemoryStream
                .ToDF().WriteStream().QueryName("testQuery").Format("console").Start();

            Assert.IsType<string>(sq.Name);

            Assert.IsType<string>(sq.Id);

            Assert.IsType<string>(sq.RunId);

            Assert.IsType<bool>(sq.IsActive());

            Assert.IsType<bool>(sq.AwaitTermination(1000));

            sq.Explain();

            Assert.Null(sq.Exception());

            sq.Stop();
        }
    }
}
