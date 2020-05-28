// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using System.IO.Compression;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Utils;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class DependencyProviderTests
    {
        [Fact]
        public void TestsUnpackPackages()
        {
            string packageFileName = "package.name.1.0.0.nupkg";
            string packageName = "package.name";
            string packageVersion = "1.0.0";

            using var emptyFileDir = new TemporaryDirectory();
            string emptyFileName = "emptyfile";
            File.Create(Path.Combine(emptyFileDir.Path, emptyFileName)).Dispose();

            using var nupkgDir = new TemporaryDirectory();
            ZipFile.CreateFromDirectory(
                emptyFileDir.Path,
                Path.Combine(nupkgDir.Path, packageFileName));

            var metadata = new DependencyProviderUtils.Metadata
            {
                AssemblyProbingPaths = new string[] { "/assembly/probe/path" },
                NativeProbingPaths = new string[] { "/native/probe/path" },
                NuGets = new DependencyProviderUtils.NuGetMetadata[]
                {
                    new DependencyProviderUtils.NuGetMetadata
                    {
                        FileName = packageFileName,
                        PackageName = packageName,
                        PackageVersion = packageVersion
                    }
                }
            };

            using var unpackDir = new TemporaryDirectory();
            string metadataFilePath =
                Path.Combine(nupkgDir.Path, DependencyProviderUtils.CreateFileName(1));
            metadata.Serialize(metadataFilePath);

            // Files located in nupkgDir
            // nuget: package.name.1.0.0.nupkg
            // metadata file: dependencyProviderMetadata_00000000000000000001
            var dependencyProvider =
                new DependencyProvider(metadataFilePath, nupkgDir.Path, unpackDir.Path);
            string expectedPackagePath =
                Path.Combine(unpackDir.Path, ".nuget", "packages", packageName, packageVersion);
            string expectedFilePath = Path.Combine(expectedPackagePath, emptyFileName);
            Assert.True(File.Exists(expectedFilePath));
        }
    }
}
