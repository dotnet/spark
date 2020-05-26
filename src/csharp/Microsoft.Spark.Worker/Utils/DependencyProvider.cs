using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Microsoft.Spark.Utils;
using DepManager = Microsoft.DotNet.DependencyManager;

namespace Microsoft.Spark.Worker.Utils
{
    internal class DependencyProvider : IDisposable
    {
        private readonly DepManager.DependencyProvider _dependencyProvider;

        public DependencyProvider(string basePath, DependencyProviderUtils.Metadata metadata)
        {
            IEnumerable<string> AssemblyProbingPaths()
            {
                foreach (string dependency in metadata.AssemblyProbingPaths)
                {
                    yield return Path.Combine(basePath, dependency);
                }
            }

            IEnumerable<string> NativeProbingRoots()
            {
                foreach (string dependency in metadata.NativeProbingPaths)
                {
                    yield return Path.Combine(basePath, dependency);
                }
            }

            _dependencyProvider = new DepManager.DependencyProvider(
                AssemblyProbingPaths,
                NativeProbingRoots);
        }

        internal static void UnpackPackages(
            string src,
            string dst,
            DependencyProviderUtils.NuGetMetadata[] nugetMetadata)
        {
            foreach (DependencyProviderUtils.NuGetMetadata metadata in nugetMetadata)
            {
                string relativePackagePath =
                    Path.Combine(metadata.PackageName.ToLower(), metadata.PackageVersion);
                var packageDirectory = new DirectoryInfo(Path.Combine(dst, relativePackagePath));
                if (!packageDirectory.Exists)
                {
                    ZipFile.ExtractToDirectory(
                        Path.Combine(src, metadata.FileName),
                        packageDirectory.FullName);
                }
            }
        }

        public void Dispose()
        {
            (_dependencyProvider as IDisposable)?.Dispose();
        }
    }
}
