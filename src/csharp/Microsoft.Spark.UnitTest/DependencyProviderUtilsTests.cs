using System.Collections;
using System.IO;
using System.Linq;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class DependencyProviderUtilsTests
    {
        [Fact]
        public void TestMetadataSerDe()
        {
            using var tempDir = new TemporaryDirectory();
            var metadata = new DependencyProviderUtils.Metadata
            {
                AssemblyProbingPaths = new string[] { "/assembly/probe/path" },
                NativeProbingPaths = new string[] { "/native/probe/path" },
                NuGets = new DependencyProviderUtils.NuGetMetadata[]
                {
                    new DependencyProviderUtils.NuGetMetadata
                    {
                        FileName = "package.name.1.0.0.nupkg",
                        PackageName = "package.name",
                        PackageVersion = "1.0.0"
                    }
                }
            };

            string serializedFilePath = Path.Combine(tempDir.Path, "serializedMetadata");
            metadata.Serialize(serializedFilePath);

            DependencyProviderUtils.Metadata deserializedMetadata =
                DependencyProviderUtils.Metadata.Deserialize(serializedFilePath);

            Assert.Equal(metadata, deserializedMetadata);
        }

        [Fact]
        public void TestFileNames()
        {
            using var tempDir = new TemporaryDirectory();
            foreach (ulong num in Enumerable.Range(0, 3).Select(x => System.Math.Pow(10, x)))
            {
                string filePath =
                    Path.Combine(tempDir.Path, DependencyProviderUtils.CreateFileName(num));
                File.Create(filePath).Dispose();
            }

            var expectedFiles = new string[] 
            {
                "dependencyProviderMetadata_00000000000000000001",
                "dependencyProviderMetadata_00000000000000000010",
                "dependencyProviderMetadata_00000000000000000100",
            };
            IOrderedEnumerable<string> actualFiles = DependencyProviderUtils
                .GetMetadataFiles(tempDir.Path)
                .Select(f => Path.GetFileName(f))
                .OrderBy(s => s);
            Assert.True(expectedFiles.SequenceEqual(actualFiles));
        }
    }
}
