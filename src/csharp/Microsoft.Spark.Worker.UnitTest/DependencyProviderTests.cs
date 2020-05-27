// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using Microsoft.Spark.Network;
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
            var packageFileName = "package.name.1.0.0.nupkg";
            var packageName = "package.name";
            var packageVersion = "1.0.0";

            using var emptyFileDir = new TemporaryDirectory();
            var emptyFileName = "emptyfile";
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
            {
                var dependencyProvider = new DependencyProvider(nupkgDir.Path, unpackDir.Path);
                Assert.False(dependencyProvider.TryLoad());
            }

            metadata.Serialize(
                Path.Combine(nupkgDir.Path, DependencyProviderUtils.CreateFileName(1)));
            {
                var dependencyProvider = new DependencyProvider(nupkgDir.Path, unpackDir.Path);
                Assert.True(dependencyProvider.TryLoad());
                string expectedPackagePath =
                    Path.Combine(unpackDir.Path, ".nuget", "packages", packageName, packageVersion);
                string expectedFilePath = Path.Combine(expectedPackagePath, emptyFileName);
                Assert.True(File.Exists(expectedFilePath));
            }

            {
                var dependencyProvider = new DependencyProvider(nupkgDir.Path, unpackDir.Path);
                Assert.False(dependencyProvider.TryLoad());
            }

            {
                metadata.Serialize(
                    Path.Combine(nupkgDir.Path, DependencyProviderUtils.CreateFileName(2)));
                var dependencyProvider = new DependencyProvider(nupkgDir.Path, unpackDir.Path);
                Assert.True(dependencyProvider.TryLoad());
            }
        }
    }
}
