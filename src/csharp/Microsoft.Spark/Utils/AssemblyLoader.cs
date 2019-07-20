using System;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;

namespace Microsoft.Spark.Utils
{
    internal class AssemblyLoader
    {
        internal static Func<string, Assembly> LoadFromFile { get; set; } = Assembly.LoadFrom;

        internal static Func<string, Assembly> LoadFromName { get; set; } = Assembly.Load;

        internal static readonly ConcurrentDictionary<string, string> s_assemblyNameToFileName =
            new ConcurrentDictionary<string, string>();

        private static readonly ConcurrentDictionary<string, Lazy<Assembly>> s_assemblyCache =
            new ConcurrentDictionary<string, Lazy<Assembly>>();

        private static ResolveEventHandler s_eventHandler = null;

        static AssemblyLoader()
        {
            InstallHandler();
        }

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

        internal static Assembly ResolveAssembly(string assemblyName)
        {
            if (s_assemblyNameToFileName.TryGetValue(assemblyName, out string assemblyFile))
            {
                return LoadAssembly(assemblyFile);
            }

            throw new FileNotFoundException($"Assembly file not found: '{assemblyName}'");
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
        internal static Assembly LoadAssembly(string assemblyName, string manifestModuleName) =>
            s_assemblyCache.GetOrAdd(
                assemblyName,
                _ => new Lazy<Assembly>(
                    () =>
                    {
                        try
                        {
                            return LoadFromName(assemblyName);
                        }
                        catch
                        {
                            s_assemblyNameToFileName.TryAdd(assemblyName, manifestModuleName);
                            return LoadAssembly(manifestModuleName);
                        }
                    })).Value;

        /// <summary>
        /// Returns the loaded assembly by probing the following locations in order:
        /// 1) The working directory
        /// 2) The directory of the application
        /// If the assembly is not found in the above locations, the exception from
        /// AssemblyLoader() will be propagated.
        /// </summary>
        /// <param name="manifestModuleName">The name of assembly to load</param>
        /// <returns>The loaded assembly</returns>
        internal static Assembly LoadAssembly(string manifestModuleName)
        {
            string currDirAsmPath =
                Path.Combine(Directory.GetCurrentDirectory(), manifestModuleName);
            if (File.Exists(currDirAsmPath))
            {
                return LoadFromFile(currDirAsmPath);
            }

            string currDomainBaseDirAsmPath =
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, manifestModuleName);
            if (File.Exists(currDomainBaseDirAsmPath))
            {
                return LoadFromFile(currDomainBaseDirAsmPath);
            }

            throw new FileNotFoundException(
                $"Assembly files not found: '{currDirAsmPath}', '{currDomainBaseDirAsmPath}'");
        }
    }
}
