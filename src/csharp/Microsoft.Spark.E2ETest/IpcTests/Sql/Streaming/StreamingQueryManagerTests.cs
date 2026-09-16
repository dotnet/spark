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
    [Trait("Category", "Streaming")]
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
            StreamingQueryManager sqm = _spark.Streams();
            StreamingQuery sq1 = null;
            StreamingQuery sq2 = null;
            try
            {
                var intMemoryStream = new MemoryStream<int>(_spark);
                sq1 = intMemoryStream
                    .ToDF().WriteStream().QueryName("intQuery").Format("console").Start();

                var stringMemoryStream = new MemoryStream<string>(_spark);
                sq2 = stringMemoryStream
                    .ToDF().WriteStream().QueryName("stringQuery").Format("console").Start();

                StreamingQuery[] streamingQueries = sqm.Active().ToArray();
                Assert.Equal(2, streamingQueries.Length);

                Assert.IsType<StreamingQuery>(sqm.Get(sq1.Id));
                Assert.IsType<StreamingQuery>(sqm.Get(sq2.Id));

                sqm.ResetTerminated();

                sqm.AwaitAnyTermination(10);
            }
            finally
            {
                try
                {
                    sq1?.Stop();
                }
                finally
                {
                    try
                    {
                        sq2?.Stop();
                    }
                    finally
                    {
                        sqm.ResetTerminated();
                    }
                }
            }
        }
    }
}
