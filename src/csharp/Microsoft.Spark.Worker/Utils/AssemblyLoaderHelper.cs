// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Utils;

#if NETCOREAPP
using System.Runtime.Loader;
#endif

namespace Microsoft.Spark.Worker.Utils
{
    internal static class AssemblyLoaderHelper
    {
        private static readonly bool s_runningREPL =
            EnvironmentUtils.GetEnvironmentVariableAsBool("DOTNET_SPARK_RUNNING_REPL");

        private static int s_stageId = int.MinValue;
        private static DependencyProvider s_dependencyProvider;

        private static readonly object s_lock = new object();

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
        /// - <see cref="DependencyProviderUtils.CreateFileName(ulong)"/>
        ///   Serialized <see cref="DependencyProviderUtils.Metadata"/> object.
        ///
        /// On the Worker, in order to resolve the nuget dependencies referenced by
        /// the dotnet-interactive session, we instantiate a
        /// <see cref="DotNet.DependencyManager.DependencyProvider"/>.
        /// This provider will register an event handler to the Assembly Load Resolving event.
        /// By using <see cref="SparkFiles.GetRootDirectory"/>, we can access the
        /// required files added to the <see cref="SparkContext"/>.
        ///
        /// Note: Because <see cref="SparkContext.AddFile(string, bool)"/> prevents
        /// overwriting/deleting files once they have been added to the
        /// <see cref="SparkContext"/>, numbered identifiers are added to relevant files:
        /// - <see cref="DependencyProviderUtils.CreateFileName(ulong)"/>
        /// </summary>
        /// <param name="stageId">The current Stage ID</param>
        internal static void RegisterAssemblyHandler(int stageId)
        {
            if (!s_runningREPL || (stageId == s_stageId))
            {
                return;
            }

            // For a given stage, it is sufficient to instantiate one DependencyProvider.
            // However, the Worker process may be reused between stages. New nuget dependencies
            // may be introduced between stages and a new DependencyProvider will need to be
            // created that can resolve them.
            lock (s_lock)
            {
                if (stageId == s_stageId)
                {
                    return;
                }
                s_stageId = stageId;

                var dependencyProvider = new DependencyProvider(
                    SparkFiles.GetRootDirectory(),
                    Directory.GetCurrentDirectory());
                if (dependencyProvider.TryLoad())
                {
                    s_dependencyProvider?.Dispose();
                    s_dependencyProvider = dependencyProvider;
                }
            }
        }
    }
}
