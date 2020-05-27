using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Microsoft.Spark.Utils;
using DepManager = Microsoft.DotNet.DependencyManager;

namespace Microsoft.Spark.Worker.Utils
{
    internal class DependencyProvider : IDisposable
    {
        private static string s_lastFileRead;

        private DepManager.DependencyProvider _dependencyProvider;
        private string _src;
        private string _dst;

        public DependencyProvider(string src, string dst)
        {
            _src = src;
            _dst = dst;
        }

        public bool TryLoad()
        {
            string metadataFile = DependencyProviderUtils.FindHighestFile(_src);

            if (string.IsNullOrEmpty(metadataFile) || metadataFile.Equals(s_lastFileRead))
            {
                return false;
            }
            s_lastFileRead = metadataFile;

            DependencyProviderUtils.Metadata metadata =
                DependencyProviderUtils.Metadata.Deserialize(metadataFile);

            string unpackPath = Path.Combine(_dst, ".nuget", "packages");
            Directory.CreateDirectory(unpackPath);

            UnpackPackages(_src, unpackPath, metadata.NuGets);

            _dependencyProvider = CreateDependencyProvider(unpackPath, metadata);

            return true;
        }

        public void Dispose()
        {
            (_dependencyProvider as IDisposable)?.Dispose();
        }

        private DepManager.DependencyProvider CreateDependencyProvider(
            string basePath,
            DependencyProviderUtils.Metadata metadata)
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

            return new DepManager.DependencyProvider(
                AssemblyProbingPaths,
                NativeProbingRoots);
        }

        private void UnpackPackages(
            string src,
            string dst,
            DependencyProviderUtils.NuGetMetadata[] nugetMetadata)
        {
            foreach (DependencyProviderUtils.NuGetMetadata metadata in nugetMetadata)
            {
                var packageDirectory = new DirectoryInfo(
                    Path.Combine(dst, metadata.PackageName.ToLower(), metadata.PackageVersion));
                if (!packageDirectory.Exists)
                {
                    ZipFile.ExtractToDirectory(
                        Path.Combine(src, metadata.FileName),
                        packageDirectory.FullName);
                }
            }
        }
    }
}
