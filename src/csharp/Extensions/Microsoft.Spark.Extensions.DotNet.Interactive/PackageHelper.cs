// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.Utility;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    internal static class PackagesHelper
    {
        private static readonly HashSet<string> s_filesCopied = new HashSet<string>();
        private static ulong s_metadataCounter = 0;

        internal static void GenerateAndAddFiles(
            string writePath,
            Action<string> fileAction)
        {
            IEnumerable<NuGetPackage> nugetPackagesToCopy = GetPackagesToCopy();

            var assemblyProbingPaths = new List<string>();
            var nativeProbingPaths = new List<string>();
            var nugetMetadata = new List<DependencyProviderUtils.NuGetMetadata>();

            foreach (NuGetPackage package in nugetPackagesToCopy)
            {
                ResolvedPackageReference resolvedPackage = package.ResolvedPackage;
                foreach (FileInfo asmPath in resolvedPackage.AssemblyPaths)
                {
                    assemblyProbingPaths.Add(
                        GetPathRelativeToPackages(
                            asmPath.FullName,
                            resolvedPackage.PackageRoot));
                }

                foreach (DirectoryInfo probePath in resolvedPackage.ProbingPaths)
                {
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

                fileAction(package.NuGetFile.FullName);
            }

            if (nugetMetadata.Count > 0)
            {
                var metadataPath =
                    Path.Combine(
                        writePath,
                        DependencyProviderUtils.CreateFileName(++s_metadataCounter));
                new DependencyProviderUtils.Metadata
                {
                    AssemblyProbingPaths = assemblyProbingPaths.ToArray(),
                    NativeProbingPaths = nativeProbingPaths.ToArray(),
                    NuGets = nugetMetadata.ToArray()
                }.Serialize(metadataPath);

                fileAction(metadataPath);
            }
        }

        private static IEnumerable<NuGetPackage> GetPackagesToCopy()
        {
            PackageRestoreContext restoreContext =
                (KernelInvocationContext.Current.HandlingKernel as ISupportNuget)
                .PackageRestoreContext;
            IEnumerable<ResolvedPackageReference> packages =
                restoreContext.ResolvedPackageReferences;
            foreach (ResolvedPackageReference package in packages)
            {
                IEnumerable<FileInfo> files =
                    package.PackageRoot.EnumerateFiles("*.nupkg", SearchOption.AllDirectories);

                foreach (FileInfo file in files)
                {
                    if (!file.Exists || s_filesCopied.Contains(file.Name))
                    {
                        continue;
                    }
                    
                    s_filesCopied.Add(file.Name);
                    yield return new NuGetPackage { ResolvedPackage = package, NuGetFile = file };
                }
            }
        }

        private static string GetPathRelativeToPackages(string file, DirectoryInfo directory)
        {
            string strippedRoot = file.Substring(directory.FullName.Length);
            string relativePath =
                Path.Combine(
                    Trim(directory.Parent.Name),
                    Trim(directory.Name),
                    Trim(strippedRoot));
            return relativePath;

            static string Trim(string path)
            {
                return path.Trim(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            }
        }

        private class NuGetPackage
        {
            public ResolvedPackageReference ResolvedPackage { get; set; }
            public FileInfo NuGetFile { get; set; }
        }
    }
}
