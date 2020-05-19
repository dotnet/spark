// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.Utility;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    internal static class PackagesHelper
    {
        private static readonly HashSet<string> s_filesCopied = new HashSet<string>();
        private static ulong s_metadataCounter = 0;

        private static PackageRestoreContext RestoreContext =>
            (KernelInvocationContext.Current.HandlingKernel as ISupportNuget)
            .PackageRestoreContext;

        internal static void GenerateAndAddFiles(
            DirectoryInfo dir,
            Action<string> fileAction)
        {
            IEnumerable<NuGetPackage> nugetPackages = GetNugetPackages();
            IEnumerable<FileInfo> filesToCopy =
                GetFilesToCopy(nugetPackages.Select(n => n.NuGetFile));

            bool newPackages = false;
            foreach (FileInfo file in filesToCopy)
            {
                newPackages = true;
                fileAction(file.FullName);
            }

            if (newPackages)
            {
                GenerateMetadata(dir.FullName, nugetPackages, fileAction);
            }
        }

        private static void GenerateMetadata(
            string path,
            IEnumerable<NuGetPackage> nugetPackages,
            Action<string> fileAction)
        {
            List<string> assemblyProbingPaths = new List<string>();
            List<string> nativeProbingPaths = new List<string>();

            PackageRestoreContext restoreContext = RestoreContext;
            IEnumerable<ResolvedPackageReference> packages =
                restoreContext.ResolvedPackageReferences;
            foreach (ResolvedPackageReference package in packages)
            {
                foreach (FileInfo asmPath in package.AssemblyPaths)
                {
                    assemblyProbingPaths.Add(
                        GetPathRelativeToPackages(asmPath.FullName, package.PackageRoot));
                }

                foreach (DirectoryInfo probePath in package.ProbingPaths)
                {
                    nativeProbingPaths.Add(
                        GetPathRelativeToPackages(probePath.FullName, package.PackageRoot));
                }
            }

            string metadataPath =
                Path.Combine(
                    path,
                    NewNumberFileName(DependencyProviderUtils.FilePattern, ++s_metadataCounter));
            using (FileStream fileStream = File.OpenWrite(metadataPath))
            {
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(
                    fileStream,
                    new DependencyProviderUtils.Metadata
                    {
                        AssemblyProbingPaths = assemblyProbingPaths.ToArray(),
                        NativeProbingPaths = nativeProbingPaths.ToArray(),
                        NuGets = nugetPackages.Select(n => n.Metadata).ToArray()
                    });
            }

            fileAction(metadataPath);
        }

        private static IEnumerable<NuGetPackage> GetNugetPackages()
        {
            PackageRestoreContext restoreContext = RestoreContext;
            IEnumerable<ResolvedPackageReference> packages =
                restoreContext.ResolvedPackageReferences;
            List<NuGetPackage> nugets = new List<NuGetPackage>();
            foreach (ResolvedPackageReference package in packages)
            {
                IEnumerable<FileInfo> files =
                    package.PackageRoot.EnumerateFiles("*.nupkg", SearchOption.AllDirectories);
                foreach (FileInfo file in files)
                {
                    nugets.Add(
                        new NuGetPackage
                        {
                            NuGetFile = file,
                            Metadata = new DependencyProviderUtils.NuGetMetadata
                            {
                                FileName = file.Name,
                                PackageName = package.PackageName,
                                PackageVersion = package.PackageVersion
                            }
                        });
                }
            }

            return nugets;
        }

        private static IEnumerable<FileInfo> GetFilesToCopy(IEnumerable<FileInfo> files)
        {
            foreach (FileInfo f in files)
            {
                if (!s_filesCopied.Contains(f.Name))
                {
                    s_filesCopied.Add(f.Name);
                    yield return f;
                }
            }
        }

        private static string NewNumberFileName(string pattern, ulong number) =>
            pattern.Replace("*", $"{number:D20}");

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
            public FileInfo NuGetFile { get; set; }
            public DependencyProviderUtils.NuGetMetadata Metadata { get; set; }
        }
    }
}
