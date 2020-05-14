using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.Utility;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    internal static class PackagesHelper
    {
        private static readonly HashSet<string> s_filesCopied = new HashSet<string>();
        private static ulong s_metadataCounter = 0;

        private static PackageRestoreContext RestoreContext =>
            (KernelInvocationContext.Current.HandlingKernel as ISupportNuget)
            .PackageRestoreContext;

        internal static void GenerateAndAddFiles(DirectoryInfo dir, Action<string, bool> fileAction)
        {
            IEnumerable<NuGetMetadata> nugets = GetNugetPackages();
            IEnumerable<FileInfo> packages = GetFilesToCopy(nugets.Select(n => n.File));
            int packageCount = 0;
            foreach (FileInfo file in packages)
            {
                packageCount++;
                fileAction(file.FullName, false);
            }

            if (packageCount > 0)
            {
                GenerateMetadata(dir.FullName, nugets, fileAction);
            }
        }

        private static void GenerateMetadata(string path,
            IEnumerable<NuGetMetadata> nugets,
            Action<string, bool> fileAction)
        {
            s_metadataCounter++;

            // Assembly probing paths
            var assemblyProbingPath =
                Path.Combine(path, NewNumberFileName("assemblyPaths_*.probe", s_metadataCounter));
            File.WriteAllLines(assemblyProbingPath, GetAssemblyProbingPaths());
            fileAction(assemblyProbingPath, false);

            // Native probing paths
            var nativeProbingPath =
                Path.Combine(path, NewNumberFileName("nativePaths_*.probe", s_metadataCounter));
            File.WriteAllLines(nativeProbingPath, GetNativeProbingPaths());
            fileAction(nativeProbingPath, false);

            // Nuget metadata
            var nugetMetadata =
                Path.Combine(path, NewNumberFileName("nugets_*.txt", s_metadataCounter));
            File.WriteAllLines(
                nugetMetadata,
                nugets.Select(n => $"{n.File.Name}/{n.Name}/{n.Version}"));
            fileAction(nugetMetadata, false);
        }

        private static IEnumerable<NuGetMetadata> GetNugetPackages()
        {
            PackageRestoreContext restoreContext = RestoreContext;
            IEnumerable<ResolvedPackageReference> packages =
                restoreContext.ResolvedPackageReferences;
            List<NuGetMetadata> nugets = new List<NuGetMetadata>();
            foreach (ResolvedPackageReference package in packages)
            {
                IEnumerable<FileInfo> files =
                    package.PackageRoot.EnumerateFiles("*.nupkg", SearchOption.AllDirectories);
                foreach (FileInfo file in files)
                {
                    nugets.Add(
                        new NuGetMetadata
                        {
                            File = file,
                            Name = package.PackageName,
                            Version = package.PackageVersion
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

        private static IEnumerable<string> GetAssemblyProbingPaths()
        {
            PackageRestoreContext restoreContext = RestoreContext;
            IEnumerable<ResolvedPackageReference> packages =
                restoreContext.ResolvedPackageReferences;
            foreach (ResolvedPackageReference package in packages)
            {
                foreach (FileInfo path in package.AssemblyPaths)
                {
                    yield return GetPathRelativeToPackages(path.FullName, package.PackageRoot);
                }
            }
        }

        private static IEnumerable<string> GetNativeProbingPaths()
        {
            PackageRestoreContext restoreContext = RestoreContext;
            IEnumerable<ResolvedPackageReference> packages =
                restoreContext.ResolvedPackageReferences;
            foreach (ResolvedPackageReference package in packages)
            {
                foreach (DirectoryInfo path in package.ProbingPaths)
                {
                    yield return GetPathRelativeToPackages(path.FullName, package.PackageRoot);
                }
            }
        }

        private struct NuGetMetadata
        {
            public FileInfo File { get; set; }
            public string Name { get; set; }
            public string Version { get; set; }
        }
    }
}
