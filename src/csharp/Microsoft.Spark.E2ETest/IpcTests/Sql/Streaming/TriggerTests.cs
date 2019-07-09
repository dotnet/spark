// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

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
        /// Test Trigger's static functions
        /// </summary>
        [Fact]
        public void TestSignatures()
        {
            Assert.IsType<Trigger>(Trigger.Once());

            Assert.IsType<Trigger>(Trigger.Continuous("1 seconds"));
            Assert.IsType<Trigger>(Trigger.Continuous(1000));

            Assert.IsType<Trigger>(Trigger.ProcessingTime("1 seconds"));
            Assert.IsType<Trigger>(Trigger.ProcessingTime(1000));
        }
    }
}
