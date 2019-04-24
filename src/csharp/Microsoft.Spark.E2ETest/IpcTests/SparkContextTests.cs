// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

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

            _ = sc.GetConf();
            _ = sc.DefaultParallelism;

            sc.SetJobDescription("job description");

            sc.SetJobGroup("group id", "description");
            sc.SetJobGroup("group id", "description", true);

            sc.ClearJobGroup();

            string filePath = TestEnvironment.ResourceDirectory + "people.txt";
            sc.AddFile(filePath);
            sc.AddFile(filePath, true);

            sc.SetCheckpointDir(TestEnvironment.ResourceDirectory);
        }
    }
}
