// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using Microsoft.DotNet.Interactive.Utility;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    internal class PackageHelper
    {
        private readonly IPackageResolver _packageResolver;
        private ulong _metadataCounter = 0;

        internal PackageHelper(IPackageResolver packageResolver)
        {
            _packageResolver = packageResolver;
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
            IEnumerable<ResolvedNuGetPackage> nugetPackagesToCopy =
                _packageResolver.GetPackagesToCopy();

            var assemblyProbingPaths = new List<string>();
            var nativeProbingPaths = new List<string>();
            var nugetMetadata = new List<DependencyProviderUtils.NuGetMetadata>();

            foreach (ResolvedNuGetPackage package in nugetPackagesToCopy)
            {
                ResolvedPackageReference resolvedPackage = package.ResolvedPackage;


                foreach (FileInfo asmPath in resolvedPackage.AssemblyPaths)
                {
                    // asmPath.FullName
                    //   /path/to/packages/package.name/package.version/lib/framework/1.dll
                    // resolvedPackage.PackageRoot
                    //   /path/to/packages/package.name/package.version/
                    // GetRelativeToPackages(..)
                    //   package.name/package.version/lib/framework/1.dll
                    assemblyProbingPaths.Add(
                        GetPathRelativeToPackages(
                            asmPath.FullName,
                            resolvedPackage.PackageRoot));
                }

                foreach (DirectoryInfo probePath in resolvedPackage.ProbingPaths)
                {
                    // probePath.FullName
                    //   /path/to/packages/package.name/package.version/
                    // resolvedPackage.PackageRoot
                    //   /path/to/packages/package.name/package.version/
                    // GetRelativeToPackages(..)
                    //   package.name/package.version
                    nativeProbingPaths.Add(
                        GetPathRelativeToPackages(
                            probePath.FullName,
                            resolvedPackage.PackageRoot));
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
                        DependencyProviderUtils.CreateFileName(++_metadataCounter));
                new DependencyProviderUtils.Metadata
                {
                    AssemblyProbingPaths = assemblyProbingPaths.ToArray(),
                    NativeProbingPaths = nativeProbingPaths.ToArray(),
                    NuGets = nugetMetadata.ToArray()
                }.Serialize(metadataPath);

                yield return metadataPath;
            }
        }

        private string GetPathRelativeToPackages(string file, DirectoryInfo directory)
        {
            string strippedRoot = file
                .Substring(directory.FullName.Length)
                .Trim(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            return Path.Combine(directory.Parent.Name, directory.Name, strippedRoot);
        }
    }
}
