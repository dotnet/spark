// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.DotNet.Interactive;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    internal class PackageResolver
    {
        private readonly SupportNugetWrapper _supportNugetWrapper;
        private readonly ConcurrentDictionary<string, byte> _filesCopied;
        private long _metadataCounter;

        internal PackageResolver(SupportNugetWrapper supportNugetWrapper)
        {
            _supportNugetWrapper = supportNugetWrapper;
            _filesCopied = new ConcurrentDictionary<string, byte>();
            _metadataCounter = 0;
        }

        /// <summary>
        /// Generates and serializes a <see cref="DependencyProviderUtils.Metadata"/> to
        /// <paramref name="writePath"/>. Returns a list of file paths which include the
        /// the serialized <see cref="DependencyProviderUtils.Metadata"/> and nuget file
        /// dependencies.
        /// </summary>
        /// <param name="writePath">Path to write metadata.</param>
        /// <returns>
        /// List of file paths of the serialized <see cref="DependencyProviderUtils.Metadata"/>
        /// and nuget file dependencies.
        /// </returns>
        internal IEnumerable<string> GetFiles(string writePath)
        {
            IEnumerable<ResolvedNuGetPackage> nugetPackagesToCopy = GetNewPackages();

            var assemblyProbingPaths = new List<string>();
            var nativeProbingPaths = new List<string>();
            var nugetMetadata = new List<DependencyProviderUtils.NuGetMetadata>();

            foreach (ResolvedNuGetPackage package in nugetPackagesToCopy)
            {
                ResolvedPackageReference resolvedPackage = package.ResolvedPackage;

                foreach (string asmPath in resolvedPackage.AssemblyPaths)
                {
                    // asmPath
                    //   /path/to/packages/package.name/package.version/lib/framework/1.dll
                    // resolvedPackage.PackageRoot
                    //   /path/to/packages/package.name/package.version/
                    // GetRelativeToPackages(..)
                    //   package.name/package.version/lib/framework/1.dll
                    assemblyProbingPaths.Add(
                        GetPathRelativeToPackages(
                            asmPath,
                            package.PackageRootDirectory));
                }

                foreach (string probePath in resolvedPackage.ProbingPaths)
                {
                    // probePath
                    //   /path/to/packages/package.name/package.version/
                    // resolvedPackage.PackageRoot
                    //   /path/to/packages/package.name/package.version/
                    // GetRelativeToPackages(..)
                    //   package.name/package.version
                    nativeProbingPaths.Add(
                        GetPathRelativeToPackages(
                            probePath,
                            package.PackageRootDirectory));
                }

                nugetMetadata.Add(
                    new DependencyProviderUtils.NuGetMetadata
                    {
                        FileName = package.NuGetFile.Name,
                        PackageName = resolvedPackage.PackageName,
                        PackageVersion = resolvedPackage.PackageVersion
                    });

                yield return package.NuGetFile.FullName;
            }

            if (nugetMetadata.Count > 0)
            {
                var metadataPath =
                    Path.Combine(
                        writePath,
                        DependencyProviderUtils.CreateFileName(
                            Interlocked.Increment(ref _metadataCounter)));
                new DependencyProviderUtils.Metadata
                {
                    AssemblyProbingPaths = assemblyProbingPaths.ToArray(),
                    NativeProbingPaths = nativeProbingPaths.ToArray(),
                    NuGets = nugetMetadata.ToArray()
                }.Serialize(metadataPath);

                yield return metadataPath;
            }
        }

        /// <summary>
        /// Return the delta of the list of packages that have been introduced
        /// since the last call.
        /// </summary>
        /// <returns>The delta of the list of packages.</returns>
        private IEnumerable<ResolvedNuGetPackage> GetNewPackages()
        {
            IEnumerable<ResolvedPackageReference> packages =
                _supportNugetWrapper.ResolvedPackageReferences;
            foreach (ResolvedPackageReference package in packages)
            {
                var packageRootDirectory = new DirectoryInfo(package.PackageRoot);
                IEnumerable<FileInfo> files =
                    packageRootDirectory.EnumerateFiles("*.nupkg", SearchOption.AllDirectories);

                foreach (FileInfo file in files)
                {
                    if (_filesCopied.TryAdd(file.Name, 1))
                    {
                        yield return new ResolvedNuGetPackage
                        {
                            ResolvedPackage = package,
                            PackageRootDirectory = packageRootDirectory,
                            NuGetFile = file
                        };
                    }
                }
            }
        }

        /// <summary>
        /// Given a <paramref name="path"/>, get the relative path to the packages directory.
        /// The package <paramref name="directory"/> is a subfolder within the packages directory.
        /// 
        /// Examples:
        /// path:
        ///  /path/to/packages/package.name/package.version/lib/framework/1.dll
        /// directory:
        ///  /path/to/packages/package.name/package.version/
        /// relative path:
        ///   package.name/package.version/lib/framework/1.dll
        /// 
        /// path:
        ///   /path/to/packages/package.name/package.version/
        /// directory:
        ///   /path/to/packages/package.name/package.version/
        /// relative path:
        ///   package.name/package.version
        /// </summary>
        /// <param name="path">The full path used to determine the relative path.</param>
        /// <param name="directory">The package directory.</param>
        /// <returns>The relative path to the packages directory.</returns>
        private string GetPathRelativeToPackages(string path, DirectoryInfo directory)
        {
            string strippedRoot = path
                .Substring(directory.FullName.Length)
                .Trim(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            return Path.Combine(directory.Parent.Name, directory.Name, strippedRoot);
        }
    }
}
