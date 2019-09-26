// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class SparkFilesTests
    {
        [Fact]
        public void TestSparkFiles()
        {
            Assert.IsType<string>(SparkFiles.Get("people.json"));
            Assert.IsType<string>(SparkFiles.GetRootDirectory());
        }
    }
}
