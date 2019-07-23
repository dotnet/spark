﻿using System;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;

namespace Microsoft.Spark.Utils
{
    internal class AssemblyLoader
    {
        internal static Func<string, Assembly> LoadFromFile { get; set; } = Assembly.LoadFrom;

        internal static Func<string, Assembly> LoadFromName { get; set; } = Assembly.Load;

        private static readonly ConcurrentDictionary<string, Assembly> s_assemblyCache =
            new ConcurrentDictionary<string, Assembly>();

        private static readonly string[] s_searchPaths =
            new[] { Directory.GetCurrentDirectory(), AppDomain.CurrentDomain.BaseDirectory };

        private static readonly string[] s_extensions =
            new[] { ".ni.dll", ".ni.exe", ".dll", ".exe", "" };

        private static ResolveEventHandler s_eventHandler = null;

        private static object s_cacheLock = new object();

        internal static void InstallHandler()
        {
            if (s_eventHandler == null)
            {
                s_eventHandler = new ResolveEventHandler(ResolveAssembly);
                AppDomain.CurrentDomain.AssemblyResolve += s_eventHandler;
            }
        }

        internal static void RemoveHandler()
        {
            if (s_eventHandler != null)
            {
                AppDomain.CurrentDomain.AssemblyResolve -= s_eventHandler;
                s_eventHandler = null;
            }
        }

        internal static Assembly ResolveAssembly(object sender, ResolveEventArgs args) =>
            ResolveAssembly(args.Name);

        /// <summary>
        /// Return the cached assembly, otherwise look in the following probing paths,
        /// searching for the simple assembly name and s_extension combination.
        /// 1) The working directory
        /// 2) The directory of the application
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
                foreach (string searchPath in s_searchPaths)
                {
                    foreach (string extension in s_extensions)
                    {
                        string assemblyPath =
                            Path.Combine(searchPath, $"{simpleAsmName}{extension}");
                        if (File.Exists(assemblyPath))
                        {
                            assembly = LoadFromFile(assemblyPath);
                            s_assemblyCache[assemblyName] = assembly;
                            return assembly;
                        }
                    }
                }

                throw new FileNotFoundException($"Assembly file not found: '{assemblyName}'");

            }
        }

        /// <summary>
        /// Return the cached assembly, otherwise attempt to load and cache the assembly
        /// in the following order:
        /// 1) Search the assemblies loaded in the current app domain.
        /// 2) Load the assembly from disk using manifestModuleName.
        /// </summary>
        /// <param name="assemblyName">The full name of the assembly</param>
        /// <param name="manifestModuleName">Name of the module that contains the assembly</param>
        /// <returns>Cached or Loaded Assembly</returns>
        internal static Assembly LoadAssembly(string assemblyName, string manifestModuleName)
        {
            lock (s_cacheLock)
            {
                if (s_assemblyCache.TryGetValue(assemblyName, out Assembly assembly))
                {
                    return assembly;
                }

                try
                {
                    assembly = LoadFromName(assemblyName);
                }
                catch
                {
                    assembly = LoadAssembly(manifestModuleName);
                }

                s_assemblyCache[assemblyName] = assembly;
                return assembly;
            }
        }

        /// <summary>
        /// Returns the loaded assembly by probing the following locations in order:
        /// 1) The working directory
        /// 2) The directory of the application
        /// </summary>
        /// <param name="manifestModuleName">The name of the assembly to load</param>
        /// <returns>The loaded assembly</returns>
        /// <exception cref="FileNotFoundException">Thrown if the assembly is not
        /// found in the probing locations.</exception>
        private static Assembly LoadAssembly(string manifestModuleName)
        {
            foreach (string searchPath in s_searchPaths)
            {
                string assemblyPath = Path.Combine(searchPath, manifestModuleName);
                if (File.Exists(assemblyPath))
                {
                    return LoadFromFile(assemblyPath);
                }
            }

            throw new FileNotFoundException(
                $"Assembly file '{manifestModuleName}' not found in: ${string.Join(",", s_searchPaths)}");
        }
    }
}
