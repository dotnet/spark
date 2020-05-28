using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Microsoft.Spark.Utils;
using DepManager = Microsoft.DotNet.DependencyManager;

namespace Microsoft.Spark.Worker.Utils
{
    /// <summary>
    /// <see cref="DependencyProvider"/> sets up and creates a new
    /// <see cref="DepManager.DependencyProvider"/>.
    ///
    /// The following steps outline the process:
    /// - Deserializes a <see cref="DependencyProviderUtils.Metadata"/> using
    ///   <see cref="DependencyProviderUtils.FindLatestFile(string)"/>
    /// - Uses <see cref="DependencyProviderUtils.Metadata.NuGets"/> to unpack required
    ///   nugets.
    /// - Uses <see cref="DependencyProviderUtils.Metadata.AssemblyProbingPaths"/> and
    ///   <see cref="DependencyProviderUtils.Metadata.NativeProbingPaths"/> to construct
    ///   a <see cref="DepManager.DependencyProvider"/>.
    /// </summary>
    internal class DependencyProvider : IDisposable
    {
        private static string s_lastFileRead;

        private DepManager.DependencyProvider _dependencyProvider;
        private readonly string _src;
        private readonly string _dst;

        internal DependencyProvider(string src, string dst)
        {
            _src = src;
            _dst = dst;
        }

        /// <summary>
        /// Try to load a <see cref="DepManager.DependencyProvider"/> if a new
        /// <see cref="DependencyProviderUtils.Metadata"/> file exists.
        /// </summary>
        /// <returns>
        /// true if new <see cref="DepManager.DependencyProvider"/> loaded, false otherwise.
        /// </returns>
        internal bool TryLoad()
        {
            string metadataFile = DependencyProviderUtils.FindLatestFile(_src);

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
