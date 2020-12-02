// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.Spark.E2ETest;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.Extensions.FileSystem.E2ETest
{
    [Collection(Constants.FileSystemTestContainerName)]
    public class FileSystemTests
    {
        private readonly SparkSession _spark;

        public FileSystemTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test that methods return the expected signature.
        /// </summary>
        [Fact]
        public void TestSignatures()
        {
            using IFileSystem fs = FileSystem.Get(_spark.SparkContext);

            using var tempDirectory = new TemporaryDirectory();
            string path = Path.Combine(tempDirectory.Path, "temp-table");
            _spark.Range(25).Write().Format("parquet").Save(path);

            Assert.True(fs.Delete(path, true));
            Assert.False(fs.Delete(path, true));
        }
    }
}
