﻿using System.IO;
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
            foreach (ulong num in Enumerable.Range(1, 20))
            {
                string filePath =
                    Path.Combine(tempDir.Path, DependencyProviderUtils.CreateFileName(num));
                File.Create(filePath).Dispose();
            }

            string expectedFile = "dependencyProviderMetadata_00000000000000000020";
            string highestFile =
                Path.GetFileName(DependencyProviderUtils.FindLatestFile(tempDir.Path));
            Assert.Equal(expectedFile, Path.GetFileName(highestFile));
        }
    }
}