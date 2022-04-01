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
        public void TestNuGetMetadataEquals()
        {
            string expectedFileName = "package.name.1.0.0.nupkg";
            string expectedPackageName = "package.name";
            string expectedPackageVersion = "1.0.0";

            var nugetMetadata = new DependencyProviderUtils.NuGetMetadata
            {
                FileName = expectedFileName,
                PackageName = expectedPackageName,
                PackageVersion = expectedPackageVersion
            };

            Assert.False(nugetMetadata.Equals(null));
            Assert.False(nugetMetadata.Equals(new DependencyProviderUtils.NuGetMetadata()));
            Assert.False(nugetMetadata.Equals(new DependencyProviderUtils.NuGetMetadata
            {
                FileName = "",
                PackageName = expectedPackageName,
                PackageVersion = expectedPackageVersion
            }));
            Assert.False(nugetMetadata.Equals(new DependencyProviderUtils.NuGetMetadata
            {
                FileName = expectedFileName,
                PackageName = "",
                PackageVersion = expectedPackageVersion
            }));
            Assert.False(nugetMetadata.Equals(new DependencyProviderUtils.NuGetMetadata
            {
                FileName = expectedFileName,
                PackageName = expectedPackageName,
                PackageVersion = ""
            }));

            Assert.True(nugetMetadata.Equals(new DependencyProviderUtils.NuGetMetadata
            {
                FileName = expectedFileName,
                PackageName = expectedPackageName,
                PackageVersion = expectedPackageVersion
            }));
        }

        [Fact]
        public void TestMetadataEquals()
        {
            string expectedAssemblyProbingPath = "/assembly/probe/path";
            string expectedNativeProbingPath = "/native/probe/path";
            var expectedNugetMetadata = new DependencyProviderUtils.NuGetMetadata
            {
                FileName = "package.name.1.0.0.nupkg",
                PackageName = "package.name",
                PackageVersion = "1.0.0"
            };

            var metadata = new DependencyProviderUtils.Metadata
            {
                AssemblyProbingPaths = new string[] { expectedAssemblyProbingPath },
                NativeProbingPaths = new string[] { expectedNativeProbingPath },
                NuGets = new DependencyProviderUtils.NuGetMetadata[] { expectedNugetMetadata }
            };

            Assert.False(metadata.Equals(null));
            Assert.False(metadata.Equals(new DependencyProviderUtils.Metadata()));
            Assert.False(metadata.Equals(new DependencyProviderUtils.Metadata
            {
                AssemblyProbingPaths = new string[] { expectedAssemblyProbingPath },
                NativeProbingPaths = new string[] { expectedNativeProbingPath, "" },
                NuGets = new DependencyProviderUtils.NuGetMetadata[] { expectedNugetMetadata }
            }));
            Assert.False(metadata.Equals(new DependencyProviderUtils.Metadata
            {
                AssemblyProbingPaths = new string[] { expectedAssemblyProbingPath },
                NativeProbingPaths = new string[] { expectedNativeProbingPath },
                NuGets = new DependencyProviderUtils.NuGetMetadata[] { expectedNugetMetadata, null }
            }));
            Assert.False(metadata.Equals(new DependencyProviderUtils.Metadata
            {
                AssemblyProbingPaths = new string[] { expectedAssemblyProbingPath, "" },
                NativeProbingPaths = new string[] { expectedNativeProbingPath },
                NuGets = new DependencyProviderUtils.NuGetMetadata[] { expectedNugetMetadata }
            }));

            Assert.True(metadata.Equals(new DependencyProviderUtils.Metadata
            {
                AssemblyProbingPaths = new string[] { expectedAssemblyProbingPath },
                NativeProbingPaths = new string[] { expectedNativeProbingPath },
                NuGets = new DependencyProviderUtils.NuGetMetadata[] { expectedNugetMetadata }
            }));
        }

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

            Assert.True(metadata.Equals(deserializedMetadata));
        }

        [Fact]
        public void TestFileNames()
        {
            using var tempDir = new TemporaryDirectory();
            foreach (long num in Enumerable.Range(0, 3).Select(x => System.Math.Pow(10, x)))
            {
                string filePath =
                    Path.Combine(tempDir.Path, DependencyProviderUtils.CreateFileName(num));
                File.Create(filePath).Dispose();
            }

            var expectedFiles = new string[] 
            {
                "dependencyProviderMetadata_0000000000000000001",
                "dependencyProviderMetadata_0000000000000000010",
                "dependencyProviderMetadata_0000000000000000100",
            };
            IOrderedEnumerable<string> actualFiles = DependencyProviderUtils
                .GetMetadataFiles(tempDir.Path)
                .Select(f => Path.GetFileName(f))
                .OrderBy(s => s);
            Assert.True(expectedFiles.SequenceEqual(actualFiles));
        }
    }
}
