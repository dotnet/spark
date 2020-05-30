// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.Commands;
using Microsoft.DotNet.Interactive.CSharp;
using Microsoft.DotNet.Interactive.Utility;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    /// <summary>
    /// A kernel extension when using .NET for Apache Spark with Microsoft.DotNet.Interactive
    /// Adds nuget and assembly dependencies to the default <see cref="SparkSession"/>
    /// using <see cref="SparkContext.AddFile(string, bool)"/>.
    /// </summary>
    public class AssemblyKernelExtension : IKernelExtension
    {
        private readonly string _runningReplEnvVar = "DOTNET_SPARK_RUNNING_REPL";
        private readonly string _tempDirEnvVar = "DOTNET_SPARK_EXTENSION_INTERACTIVE_TMPDIR";

        /// <summary>
        /// Called by the Microsoft.DotNet.Interactive Assembly Extension Loader.
        /// </summary>
        /// <param name="kernel">The kernel calling this method.</param>
        public Task OnLoadAsync(IKernel kernel)
        {
            if (kernel is CompositeKernel kernelBase)
            {
                Environment.SetEnvironmentVariable(_runningReplEnvVar, "true");

                string envTempDir = Environment.GetEnvironmentVariable(_tempDirEnvVar);
                string tempDirBasePath = string.IsNullOrEmpty(envTempDir) ?
                    Directory.GetCurrentDirectory() :
                    envTempDir;

                if (!PackagesHelper.IsPathValid(tempDirBasePath))
                {
                    throw new Exception($"[{GetType().Name}] Spaces in " +
                            $"'{tempDirBasePath}' is unsupported. Set the {_tempDirEnvVar} " +
                            "environment variable to control the base path. Please see " +
                            "https://issues.apache.org/jira/browse/SPARK-30126 and " +
                            "https://github.com/apache/spark/pull/26773 for more details");
                }

                DirectoryInfo tempDir = Directory.CreateDirectory(
                    Path.Combine(tempDirBasePath, Path.GetRandomFileName()));
                kernelBase.RegisterForDisposal(new DisposableDirectory(tempDir));           

                kernelBase.AddMiddleware(async (command, context, next) =>
                {
                    if ((context.HandlingKernel is CSharpKernel kernel) &&
                        command is SubmitCode &&
                        TryGetSparkSession(out SparkSession sparkSession))
                    {
                        Compilation preCompilation = kernel.ScriptState.Script.GetCompilation();

                        string assemblyName =
                            AssemblyLoader.NormalizeAssemblyName(preCompilation.AssemblyName);
                        string assemblyPath = Path.Combine(tempDir.FullName, $"{assemblyName}.dll");
                        if (!File.Exists(assemblyPath))
                        {
                            FileSystemExtensions.Emit(preCompilation, assemblyPath);
                            SparkSession.Active().SparkContext.AddFile(assemblyPath);
                        }

                        PackagesHelper.GenerateAndAddFiles(
                            tempDir.FullName,
                            s => SparkSession.Active().SparkContext.AddFile(s, false));
                    }

                    await next(command, context);
                });
            }

            return Task.CompletedTask;
        }

        private bool TryGetSparkSession(out SparkSession sparkSession)
        {
            sparkSession = SparkSession.GetDefaultSession();
            return sparkSession != null;
        }
    }
}
