// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Utils;

#if NETCOREAPP
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Loader;
using System.Linq;
using Microsoft.DotNet.DependencyManager;
#endif

namespace Microsoft.Spark.Worker.Utils
{
    internal static class AssemblyLoaderHelper
    {
        private static bool s_initialized = false;

#if NETCOREAPP
        private static DependencyProvider _dependencyProvider;
#endif

        internal static void Setup()
        {
            if (s_initialized)
            {
                return;
            }

#if NETCOREAPP
            RegisterReplAssemblyHandler();
            AssemblyLoader.LoadFromFile = AssemblyLoadContext.Default.LoadFromAssemblyPath;
            AssemblyLoadContext.Default.Resolving += (assemblyLoadContext, assemblyName) =>
                AssemblyLoader.ResolveAssembly(assemblyName.FullName);
#else
            AppDomain.CurrentDomain.AssemblyResolve += (object sender, ResolveEventArgs args) =>
                AssemblyLoader.ResolveAssembly(args.Name);
#endif

            s_initialized = true;
        }

#if NETCOREAPP
        private static void RegisterReplAssemblyHandler()
        {
            if (!EnvironmentUtils.GetEnvironmentVariableAsBool("DOTNET_REPL"))
            {
                return;
            }

            string sparkFilesPath = SparkFiles.GetRootDirectory();
            string assemblyPathsFile = FindHighestFile(sparkFilesPath, "assemblyPaths_*.probe");
            string nativeDepenciesFile = FindHighestFile(sparkFilesPath, "nativePaths_*.probe");

            if (string.IsNullOrEmpty(assemblyPathsFile) ||
                string.IsNullOrEmpty(nativeDepenciesFile))
            {
                return;
            }

            string[] assemblyDependencies = File.ReadAllLines(assemblyPathsFile);
            string[] nativeDependencies = File.ReadAllLines(nativeDepenciesFile);

            string unpackPath =
                Path.Combine(Directory.GetCurrentDirectory(), Path.Combine(".nuget", "packages"));
            Directory.CreateDirectory(unpackPath);

            IEnumerable<string> AssemblyProbingPaths()
            {
                foreach (string dependency in assemblyDependencies)
                {
                    yield return Path.Combine(unpackPath, dependency);
                }
            }

            IEnumerable<string> NativeProbingRoots()
            {
                foreach (string dependency in nativeDependencies)
                {
                    yield return Path.Combine(unpackPath, dependency);
                }
            }

            UnpackPackages(sparkFilesPath, unpackPath);
            _dependencyProvider = new DependencyProvider(AssemblyProbingPaths, NativeProbingRoots);
        }

        private static string FindHighestFile(string src, string pattern)
        {
            string[] files = Directory.GetFiles(src, pattern);
            if (files.Length > 0)
            {
                Array.Sort(files);
                return files.Last();
            }

            return null;
        }

        private static void UnpackPackages(string src, string dst)
        {
            var srcDir = new DirectoryInfo(src);
            FileInfo[] packages = srcDir.GetFiles("*.nupkg");
            foreach (FileInfo package in packages)
            {
                string name = Path.GetFileNameWithoutExtension(package.Name);
                int build = name.LastIndexOf(".");
                int minor = (build >= 0) ? name.LastIndexOf(".", build - 1) : -1;
                int major = (minor >= 0) ? name.LastIndexOf(".", minor - 1) : -1;
                if (major >= 0)
                {
                    string packageName = name.Substring(0, major);
                    string packageVersion = name.Substring(major + 1);
                    var packageDirectory = new DirectoryInfo(
                        Path.Combine(dst, Path.Combine(packageName, packageVersion)));
                    if (!packageDirectory.Exists)
                    {
                        ZipFile.ExtractToDirectory(package.FullName, packageDirectory.FullName);
                    }
                }
            }
        }
#endif
    }
}
