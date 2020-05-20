// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Utils;

#if NETCOREAPP
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Loader;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.DotNet.DependencyManager;
#endif

namespace Microsoft.Spark.Worker.Utils
{
    internal static class AssemblyLoaderHelper
    {
#if NETCOREAPP
        private static int s_stageId = int.MinValue;
        private static DependencyProvider s_dependencyProvider;
        private static object s_lock = new Object();
#endif

        internal static void Initialize()
        {
#if NETCOREAPP
            AssemblyLoader.LoadFromFile = AssemblyLoadContext.Default.LoadFromAssemblyPath;
            AssemblyLoadContext.Default.Resolving += (assemblyLoadContext, assemblyName) =>
                AssemblyLoader.ResolveAssembly(assemblyName.FullName);
#else
            AppDomain.CurrentDomain.AssemblyResolve += (object sender, ResolveEventArgs args) =>
                AssemblyLoader.ResolveAssembly(args.Name);
#endif
        }

#if NETCOREAPP
        internal static void RegisterAssemblyHandler(int stageId)
        {
            if (!EnvironmentUtils.GetEnvironmentVariableAsBool("DOTNET_SPARK_REPL_MODE") ||
                (stageId == s_stageId))
            {
                return;
            }

            lock (s_lock)
            {
                if (stageId == s_stageId)
                {
                    return;
                }

                string sparkFilesPath = SparkFiles.GetRootDirectory();
                string metadataFile =
                    FindHighestFile(sparkFilesPath, DependencyProviderUtils.FilePattern);

                if (string.IsNullOrEmpty(metadataFile))
                {
                    return;
                }

                using FileStream fileStream = File.OpenRead(metadataFile);
                BinaryFormatter formatter = new BinaryFormatter();
                DependencyProviderUtils.Metadata metadata =
                    (DependencyProviderUtils.Metadata)formatter.Deserialize(fileStream);

                string unpackPath =
                    Path.Combine(Directory.GetCurrentDirectory(), Path.Combine(".nuget", "packages"));
                Directory.CreateDirectory(unpackPath);

                IEnumerable<string> AssemblyProbingPaths()
                {
                    foreach (string dependency in metadata.AssemblyProbingPaths)
                    {
                        yield return Path.Combine(unpackPath, dependency);
                    }
                }

                IEnumerable<string> NativeProbingRoots()
                {
                    foreach (string dependency in metadata.NativeProbingPaths)
                    {
                        yield return Path.Combine(unpackPath, dependency);
                    }
                }

                UnpackPackages(sparkFilesPath, unpackPath, metadata.NuGets);

                (s_dependencyProvider as IDisposable)?.Dispose();
                s_dependencyProvider = new DependencyProvider(AssemblyProbingPaths, NativeProbingRoots);
                s_stageId = stageId;
            }
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

        private static void UnpackPackages(
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
#endif
    }
}
