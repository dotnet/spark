// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.DotNet.Interactive.Utility;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Moq;
using Xunit;

namespace Microsoft.Spark.Extensions.DotNet.Interactive.UnitTest
{
    public class PackageHelperTests
    {
        [Fact]
        public void TestPackageHelper()
        {
            string basePath = new FileInfo(".").FullName;

            var mockPackageResolver = new Mock<IPackageResolver>();
            string packageName = "package.name";
            string packageVersion = "0.1.0";
            string packageRootPath =
                Path.Combine(basePath, "path", "to", "packages", packageName, packageVersion);
            string packageFrameworkPath = Path.Combine(packageRootPath, "lib", "framework");
            FileInfo nugetFile = new FileInfo(
                Path.Combine(packageRootPath, $"{packageName}.{packageVersion}.nupkg"));

            IReadOnlyList<FileInfo> assemblyPaths = new List<FileInfo>
            {
                new FileInfo(Path.Combine(packageFrameworkPath, "1.dll")),
                new FileInfo(Path.Combine(packageFrameworkPath, "2.dll"))
            }.AsReadOnly();

            IReadOnlyList<DirectoryInfo> probingPaths =
                new List<DirectoryInfo> { new DirectoryInfo(packageRootPath) }.AsReadOnly();

            mockPackageResolver
                .Setup(m => m.GetPackagesToCopy())
                .Returns(new ResolvedNuGetPackage[]
                {
                    new ResolvedNuGetPackage
                    {
                        ResolvedPackage = new ResolvedPackageReference(
                            packageName,
                            packageVersion,
                            assemblyPaths,
                            new DirectoryInfo(packageRootPath),
                            probingPaths),
                        NuGetFile = nugetFile
                    }
                }.AsEnumerable());

            using var tempDir = new TemporaryDirectory();
            PackageHelper packageHelper = new PackageHelper(mockPackageResolver.Object);
            IEnumerable<string> files = packageHelper.GetFiles(tempDir.Path);

            string metadataFilePath =
                Path.Combine(tempDir.Path, DependencyProviderUtils.CreateFileName(1));
            var expectedFiles = new string[]
            {
                nugetFile.FullName,
                metadataFilePath
            };
            Assert.True(expectedFiles.SequenceEqual(files));
            Assert.True(File.Exists(metadataFilePath));

            DependencyProviderUtils.Metadata actualMetadata =
                DependencyProviderUtils.Metadata.Deserialize(metadataFilePath);
            var expectedMetadata = new DependencyProviderUtils.Metadata
            {
                AssemblyProbingPaths = new string[]
                {
                    Path.Combine(packageName, packageVersion, "lib", "framework", "1.dll"),
                    Path.Combine(packageName, packageVersion, "lib", "framework", "2.dll")
                },
                NativeProbingPaths = new string[]
                {
                    Path.Combine(packageName, packageVersion)
                },
                NuGets = new DependencyProviderUtils.NuGetMetadata[]
                {
                    new DependencyProviderUtils.NuGetMetadata
                    {
                        FileName = $"{packageName}.{packageVersion}.nupkg",
                        PackageName = packageName,
                        PackageVersion = packageVersion
                    }
                }
            };
            Assert.True(expectedMetadata.Equals(actualMetadata));
        }
    }
}
