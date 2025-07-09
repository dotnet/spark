// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.DotNet.Interactive;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Moq;
using Xunit;

namespace Microsoft.Spark.Extensions.DotNet.Interactive.UnitTest
{
    public class PackageResolverTests
    {
        [Fact]
        public void TestPackageResolver()
        {
            using var tempDir = new TemporaryDirectory();

            string packageName = "package.name";
            string packageVersion = "0.1.0";
            string packageRootPath =
                Path.Combine(tempDir.Path, "path", "to", "packages", packageName, packageVersion);
            string packageFrameworkPath = Path.Combine(packageRootPath, "lib", "framework");

            Directory.CreateDirectory(packageRootPath);
            var nugetFile = new FileInfo(
                Path.Combine(packageRootPath, $"{packageName}.{packageVersion}.nupkg"));
            using (File.Create(nugetFile.FullName))
            {
            }
            
            var assemblyPaths = new string[]
            {
                Path.Combine(packageFrameworkPath, "1.dll"),
                Path.Combine(packageFrameworkPath, "2.dll")
            };
            var probingPaths = new string[] { packageRootPath };

            var mockSupportNugetWrapper = new Mock<ReferencedPackagesExtractor>(null);
            mockSupportNugetWrapper
                .SetupGet(m => m.ResolvedPackageReferences)
                .Returns(new ResolvedPackageReference[]
                {
                    new ResolvedPackageReference(
                        packageName,
                        packageVersion,
                        assemblyPaths,
                        packageRootPath,
                        probingPaths) 
                });

            var packageResolver = new PackageResolver(mockSupportNugetWrapper.Object);
            string[] actualFiles = packageResolver.GetFiles(tempDir.Path).ToArray();
            string actualNugetPath = actualFiles[0];
            string actualMetadataPath = actualFiles[1];
            string actualMetadataFilename = Path.GetFileName(actualMetadataPath);

            Assert.Equal(nugetFile.FullName, actualNugetPath);
            Assert.StartsWith(tempDir.Path, actualMetadataPath);
            Assert.Matches("dependencyProviderMetadata_[a-f\\d]{8}00000000001", actualMetadataFilename);
            Assert.True(File.Exists(actualMetadataPath));

            DependencyProviderUtils.Metadata actualMetadata =
                DependencyProviderUtils.Metadata.Deserialize(actualMetadataPath);
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
            Assert.Equal(expectedMetadata, actualMetadata);
        }
    }
}
