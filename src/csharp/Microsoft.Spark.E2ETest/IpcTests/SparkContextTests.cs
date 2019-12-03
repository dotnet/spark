// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.E2ETest.Utils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class SparkContextTests
    {
        /// <summary>
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        /// <remarks>
        /// For the RDD related tests, refer to <see cref="RDDTests"/>.
        /// </remarks>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            SparkContext sc = SparkContext.GetOrCreate(new SparkConf());

            Assert.IsType<SparkConf>(sc.GetConf());
            Assert.IsType<int>(sc.DefaultParallelism);

            sc.SetJobDescription("job description");

            sc.SetLogLevel(SparkContext.LogLevel.ALL);
            sc.SetLogLevel(SparkContext.LogLevel.DEBUG);
            sc.SetLogLevel(SparkContext.LogLevel.ERROR);
            sc.SetLogLevel(SparkContext.LogLevel.FATAL);
            sc.SetLogLevel(SparkContext.LogLevel.INFO);
            sc.SetLogLevel(SparkContext.LogLevel.OFF);
            sc.SetLogLevel(SparkContext.LogLevel.TRACE);
            sc.SetLogLevel(SparkContext.LogLevel.WARN);

            sc.SetJobGroup("group id", "description");
            sc.SetJobGroup("group id", "description", true);

            sc.ClearJobGroup();

            string filePath = $"{TestEnvironment.ResourceDirectory}people.txt";
            sc.AddFile(filePath);
            sc.AddFile(filePath, true);

            using var tempDir = new TemporaryDirectory();
            sc.SetCheckpointDir(TestEnvironment.ResourceDirectory);
        }
    }
}
