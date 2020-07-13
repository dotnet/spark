// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Utils
{
    internal static class AssemblySearchPathResolver
    {
        internal const string AssemblySearchPathsEnvVarName = "DOTNET_ASSEMBLY_SEARCH_PATHS";

        /// <summary>
        /// Returns the paths to search when loading assemblies in the following order of
        /// precedence:
        /// 1) Comma-separated paths specified in DOTNET_ASSEMBLY_SEARCH_PATHS environment
        /// variable. Note that if a path starts with ".", the working directory will be prepended.
        /// 2) The path of the files added through
        /// <see cref="SparkContext.AddFile(string, bool)"/>.
        /// 3) The working directory.
        /// 4) The directory of the application.
        /// </summary>
        /// <remarks>
        /// The reason that the working directory has higher precedence than the directory
        /// of the application is for cases when spark is launched on YARN. The executors are run
        /// inside 'containers' and files that are passed via 'spark-submit --files' will be pushed
        /// to these 'containers'. This path is the working directory and the 1st probing path that
        /// will be checked.
        /// </remarks>
        /// <returns>Assembly search paths</returns>
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

            string sparkFilesPath = SparkFiles.GetRootDirectory();
            if (!string.IsNullOrWhiteSpace(sparkFilesPath))
            {
                searchPaths.Add(sparkFilesPath);
            }

            searchPaths.Add(Directory.GetCurrentDirectory());
            searchPaths.Add(AppDomain.CurrentDomain.BaseDirectory);

            return searchPaths.ToArray();
        }
    }

    internal static class AssemblyLoader
    {
        internal static Func<string, Assembly> LoadFromFile { get; set; } = Assembly.LoadFrom;

        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(AssemblyLoader));

        private static readonly Dictionary<string, Assembly> s_assemblyCache =
            new Dictionary<string, Assembly>();

        // Lazily evaluate the assembly search paths because it has a dependency on SparkFiles.
        private static readonly Lazy<string[]> s_searchPaths =
            new Lazy<string[]>(() => AssemblySearchPathResolver.GetAssemblySearchPaths());

        private static readonly string[] s_extensions =
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ?
                new[] { ".dll", ".exe", ".ni.dll", ".ni.exe" } :
                new[] { ".dll", ".ni.dll" };

        private static readonly object s_cacheLock = new object();

        // Roslyn generates assembly names with characters that cause issues.
        // The generated name contains *, a reserved character in Windows,
        // and #, which causes problems when used with SparkContext.AddFile.
        // https://github.com/dotnet/roslyn/blob/da63493c37e4a450076d6dac02044bf0fcdbcc50/src/Scripting/Core/ScriptBuilder.cs#L51
        private static readonly Regex s_roslynAssemblyNameRegex =
            new Regex(
                "^\u211B\\*([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})#([0-9]+-[0-9]+)",
                RegexOptions.Compiled | RegexOptions.CultureInvariant);

        /// <summary>
        /// Return the cached assembly, otherwise attempt to load and cache the assembly
        /// by searching for the assembly filename in the search paths.
        /// </summary>
        /// <param name="assemblyName">The full name of the assembly</param>
        /// <param name="assemblyFileName">Name of the file that contains the assembly</param>
        /// <returns>Cached or Loaded Assembly or null if not found</returns>
        internal static Assembly LoadAssembly(string assemblyName, string assemblyFileName)
        {
            // assemblyFileName is empty when serializing a UDF from within the REPL.
            if (string.IsNullOrWhiteSpace(assemblyFileName))
            {
                return ResolveAssembly(assemblyName);
            }

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

                s_logger.LogWarn(
                    string.Format(
                        "Assembly '{0}' file not found '{1}' in '{2}'",
                        assemblyName,
                        assemblyFileName,
                        string.Join(",", s_searchPaths.Value)));

                return null;
            }
        }

        /// <summary>
        /// Return the cached assembly, otherwise look in the probing paths returned
        /// by AssemblySearchPathResolver, searching for the simple assembly name and
        /// s_extension combination.
        /// </summary>
        /// <param name="assemblyName">The fullname of the assembly to load</param>
        /// <returns>The loaded assembly or null if not found</returns>
        internal static Assembly ResolveAssembly(string assemblyName)
        {
            lock (s_cacheLock)
            {
                if (s_assemblyCache.TryGetValue(assemblyName, out Assembly assembly))
                {
                    return assembly;
                }

                string simpleAsmName =
                    NormalizeAssemblyName(new AssemblyName(assemblyName).Name);
                foreach (string extension in s_extensions)
                {
                    string assemblyFileName = $"{simpleAsmName}{extension}";
                    if (TryLoadAssembly(assemblyFileName, ref assembly))
                    {
                        s_assemblyCache[assemblyName] = assembly;
                        return assembly;
                    }
                }

                s_logger.LogWarn(
                    string.Format(
                        "Assembly '{0}' file not found '{1}[{2}]' in '{3}'",
                        assemblyName,
                        simpleAsmName,
                        string.Join(",", s_extensions),
                        string.Join(",", s_searchPaths.Value)));

                return null;
            }
        }

        /// <summary>
        /// Returns the loaded assembly by probing paths returned by AssemblySearchPathResolver.
        /// </summary>
        /// <param name="assemblyFileName">Name of the file that contains the assembly</param>
        /// <param name="assembly">The loaded assembly.</param>
        /// <returns>True if assembly is loaded, false otherwise.</returns>
        private static bool TryLoadAssembly(string assemblyFileName, ref Assembly assembly)
        {
            foreach (string searchPath in s_searchPaths.Value)
            {
                var assemblyFile = new FileInfo(Path.Combine(searchPath, assemblyFileName));
                if (assemblyFile.Exists)
                {
                    try
                    {
                        assembly = LoadFromFile(assemblyFile.FullName);
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

        /// <summary>
        /// Normalizes the assemblyName by removing characters known to cause
        /// issues. This is useful in situations where the assemblyName is
        /// automatically generated, ie the Roslyn compiler used in the REPL
        /// generates an assembly name that contains * and #.
        /// </summary>
        /// <param name="assemblyName">Assembly name</param>
        /// <returns>Normalized assembly name</returns>
        internal static string NormalizeAssemblyName(string assemblyName)
        {
            // Check if the assembly name follows the Roslyn naming convention.
            // Roslyn assembly name: "\u211B*4b31b71b-d4bd-4642-9f63-eef5f5d99197#1-14"
            // Normalized Roslyn assembly name: "4b31b71b-d4bd-4642-9f63-eef5f5d99197-1-14"
            Match match = s_roslynAssemblyNameRegex.Match(assemblyName);
            return match.Success ?
                $"{match.Groups[1].Value}-{match.Groups[2].Value}" :
                assemblyName;
        }
    }
}
