using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.Utility;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    static class PackagesHelper
    {
        private static readonly HashSet<string> s_filesCopied = new HashSet<string>();
        private static ulong s_probeFileCounter = 0;

        private static PackageRestoreContext RestoreContext =>
            (KernelInvocationContext.Current.HandlingKernel as ISupportNuget)
            .PackageRestoreContext;

        public static void DoWork(string path, Action<string, bool> action)
        {
            IEnumerable<NuGetMetadata> nugets = GetNugetPackages();
            IEnumerable<FileInfo> packages = GetFilesToCopy(nugets.Select(n => n.File));
            int packageCount = 0;
            foreach (FileInfo file in packages)
            {
                packageCount++;
                action(file.FullName, false);
            }

            if (packageCount > 0)
            {
                GenerateMetadata(path, nugets, action);
            }
        }

        private static void GenerateMetadata(string path,
            IEnumerable<NuGetMetadata> nugets,
            Action<string, bool> action)
        {
            s_probeFileCounter++;

            // Assembly probing paths
            var assemblyProbingPath =
                Path.Combine(path, NewNumberFileName("assemblyPaths_*.probe", s_probeFileCounter));
            File.WriteAllLines(assemblyProbingPath, GetAssemblyProbingPaths());
            action(assemblyProbingPath, false);

            // Native probing paths
            var nativeProbingPath =
                Path.Combine(path, NewNumberFileName("nativePaths_*.probe", s_probeFileCounter));
            File.WriteAllLines(nativeProbingPath, GetNativeProbingPaths());
            action(nativeProbingPath, false);

            // Nuget metadata
            var nugetMetadata =
                Path.Combine(path, NewNumberFileName("nugets_*.txt", s_probeFileCounter));
            File.WriteAllLines(
                nugetMetadata,
                nugets.Select(n => $"{n.File.Name}/{n.Name}/{n.Version}"));
            action(nugetMetadata, false);
        }

        private static IEnumerable<NuGetMetadata> GetNugetPackages()
        {
            PackageRestoreContext restoreContext = RestoreContext;
            IEnumerable<ResolvedPackageReference> packages =
                restoreContext.ResolvedPackageReferences;
            List<NuGetMetadata> nugets = new List<NuGetMetadata>();
            foreach (ResolvedPackageReference package in packages)
            {
                DirectoryInfo dir = package.PackageRoot;
                foreach (FileInfo file in dir.EnumerateFiles("*.nupkg", SearchOption.AllDirectories))
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
