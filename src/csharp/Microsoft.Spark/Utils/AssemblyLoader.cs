// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Microsoft.Spark.Utils
{
    internal static class AssemblySearchPathResolver
    {
        internal const string AssemblySearchPathsEnvVarName = "DOTNET_ASSEMBLY_SEARCH_PATHS";

        // Returns the paths to search when loading assemblies in the following order of
        // precedence:
        // 1) Comma-separated paths specified in DOTNET_ASSEMBLY_SEARCH_PATHS environment variable.
        //    - Note that if a path starts with ".", the working directory will be prepended.
        // 2) The working directory.
        // 3) The directory of the application.
        internal static string[] GetAssemblySearchPaths()
        {
            var searchPaths = new List<string>();
            string searchPathsStr =
                Environment.GetEnvironmentVariable(AssemblySearchPathsEnvVarName);

            if (!string.IsNullOrEmpty(searchPathsStr))
            {
                foreach (string searchPath in searchPathsStr.Split(','))
                {
                    string trimmedSearchPath = searchPath.Trim();
                    if (trimmedSearchPath.StartsWith("."))
                    {
                        searchPaths.Add(
                            Path.Combine(Directory.GetCurrentDirectory(), trimmedSearchPath));
                    }
                    else
                    {
                        searchPaths.Add(trimmedSearchPath);
                    }
                }
            }

            searchPaths.Add(Directory.GetCurrentDirectory());
            searchPaths.Add(AppDomain.CurrentDomain.BaseDirectory);

            return searchPaths.ToArray();
        }
    }

    internal static class AssemblyLoader
    {
        internal static Func<string, Assembly> LoadFromFile { get; set; } = Assembly.LoadFrom;

        internal static Func<string, Assembly> LoadFromName { get; set; } = Assembly.Load;

        private static readonly Dictionary<string, Assembly> s_assemblyCache =
            new Dictionary<string, Assembly>();

        private static readonly string[] s_searchPaths =
            AssemblySearchPathResolver.GetAssemblySearchPaths();

        private static readonly string[] s_extensions =
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ?
                    new[] { ".dll", ".exe", ".ni.dll", ".ni.exe" } :
                    new[] { ".dll", ".ni.dll" };

        private static readonly object s_cacheLock = new object();

        /// <summary>
        /// Return the cached assembly, otherwise attempt to load and cache the assembly
        /// by searching for the assembly filename in the search paths.
        /// </summary>
        /// <param name="assemblyName">The full name of the assembly</param>
        /// <param name="assemblyFileName">Name of the file that contains the assembly</param>
        /// <returns>Cached or Loaded Assembly</returns>
        /// <exception cref="FileNotFoundException">Thrown if the assembly is not
        /// found.</exception>
        internal static Assembly LoadAssembly(string assemblyName, string assemblyFileName)
        {
            lock (s_cacheLock)
            {
                if (s_assemblyCache.TryGetValue(assemblyName, out Assembly assembly))
                {
                    return assembly;
                }

                if (TryLoadAssembly(assemblyFileName, ref assembly))
                {
                    s_assemblyCache[assemblyName] = assembly;
                    return assembly;
                }

                throw new FileNotFoundException($"Assembly '{assemblyName}' file not found: '{assemblyFileName}'");
            }
        }

        /// <summary>
        /// Return the cached assembly, otherwise look in the probing paths returned
        /// by AssemblySearchPathResolver, searching for the simple assembly name and
        /// s_extension combination.
        /// </summary>
        /// <param name="assemblyName">The fullname of the assembly to load</param>
        /// <returns>The loaded assembly</returns>
        /// <exception cref="FileNotFoundException">Thrown if the assembly is not
        /// found.</exception>
        internal static Assembly ResolveAssembly(string assemblyName)
        {
            lock (s_cacheLock)
            {
                if (s_assemblyCache.TryGetValue(assemblyName, out Assembly assembly))
                {
                    return assembly;
                }

                string simpleAsmName = new AssemblyName(assemblyName).Name;
                foreach (string extension in s_extensions)
                {
                    string assemblyFileName = $"{simpleAsmName}{extension}";
                    if (TryLoadAssembly(assemblyFileName, ref assembly))
                    {
                        s_assemblyCache[assemblyName] = assembly;
                        return assembly;
                    }
                }

                throw new FileNotFoundException($"Assembly file not found: '{assemblyName}'");
            }
        }

        /// <summary>
        /// Returns the loaded assembly by probing the following locations in order:
        /// 1) The working directory
        /// 2) The directory of the application
        /// </summary>
        /// <remarks>
        /// The probing order is important in cases when spark is launched on
        /// YARN. The executors are run inside 'containers' and files that are passed
        /// via 'spark-submit --files' will be pushed to these 'containers'. This path
        /// is the working directory and the 1st probing path that will be checked.
        /// </remarks>
        /// <param name="assemblyFileName">Name of the file that contains the assembly</param>
        /// <param name="assembly">The loaded assembly.</param>
        /// <returns>True if assembly is loaded, false otherwise.</returns>
        private static bool TryLoadAssembly(string assemblyFileName, ref Assembly assembly)
        {
            foreach (string searchPath in s_searchPaths)
            {
                string assemblyPath = Path.Combine(searchPath, assemblyFileName);
                if (File.Exists(assemblyPath))
                {
                    try
                    {
                        assembly = LoadFromFile(assemblyPath);
                        return true;
                    }
                    catch (Exception ex) when (
                        ex is FileLoadException ||
                        ex is BadImageFormatException)
                    {
                        // Ignore invalid assemblies.
                    }
                }
            }

            return false;
        }
    }
}
