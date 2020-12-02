using System.IO;
using Microsoft.Spark.E2ETest;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.Extensions.FileSystem.E2ETest
{
    [Collection("Spark E2E Tests")]
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
