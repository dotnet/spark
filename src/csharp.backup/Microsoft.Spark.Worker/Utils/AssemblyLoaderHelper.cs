// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.IO;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Services;
using Microsoft.Spark.Utils;

#if NETCOREAPP
using System.Runtime.Loader;
#endif

namespace Microsoft.Spark.Worker.Utils
{
    internal static class AssemblyLoaderHelper
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(AssemblyLoaderHelper));

        // A mapping between a metadata file's path to its respective DependencyProvider.
        private static readonly ConcurrentDictionary<string, Lazy<DependencyProvider>>
            s_dependencyProviders = new ConcurrentDictionary<string, Lazy<DependencyProvider>>();

        private static readonly bool s_runningREPL = SparkEnvironment.ConfigurationService.IsRunningRepl();

        /// <summary>
        /// Register the AssemblyLoader.ResolveAssembly handler to handle the
        /// event when assemblies fail to load in the current assembly load context.
        /// </summary>
        static AssemblyLoaderHelper()
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

        /// <summary>
        /// In a dotnet-interactive REPL session (driver), nuget dependencies will be
        /// systematically added using <see cref="SparkContext.AddFile(string, bool)"/>.
        ///
        /// These files include:
        /// - "{packagename}.{version}.nupkg"
        ///   The nuget packages
        /// - <see cref="DependencyProviderUtils.CreateFileName(long)"/>
        ///   Serialized <see cref="DependencyProviderUtils.Metadata"/> object.
        ///
        /// On the Worker, in order to resolve the nuget dependencies referenced by
        /// the dotnet-interactive session, we instantiate a
        /// <see cref="DotNet.DependencyManager.DependencyProvider"/>.
        /// This provider will register an event handler to the Assembly Load Resolving event.
        /// By using <see cref="SparkFiles.GetRootDirectory"/>, we can access the
        /// required files added to the <see cref="SparkContext"/>.
        /// </summary>
        internal static void RegisterAssemblyHandler()
        {
            if (!s_runningREPL)
            {
                return;
            }

            string sparkFilesPath = SparkFiles.GetRootDirectory();
            string[] metadataFiles =
                DependencyProviderUtils.GetMetadataFiles(sparkFilesPath);
            foreach (string metdatafile in metadataFiles)
            {
                // The execution of the delegate passed to GetOrAdd is not guaranteed to run once.
                // Multiple Lazy objects may be created, but only one of them will be added to the
                // ConcurrentDictionary. The Lazy value is retrieved to materialize the
                // DependencyProvider object if it hasn't already been created.
                Lazy<DependencyProvider> dependecyProvider = s_dependencyProviders.GetOrAdd(
                    metdatafile,
                    mdf => new Lazy<DependencyProvider>(
                        () =>
                        {
                            s_logger.LogInfo($"Creating {nameof(DependencyProvider)} using {mdf}");
                            return new DependencyProvider(
                                mdf,
                                sparkFilesPath,
                                Directory.GetCurrentDirectory());
                        }));
                _ = dependecyProvider.Value;
            }
        }
    }
}
