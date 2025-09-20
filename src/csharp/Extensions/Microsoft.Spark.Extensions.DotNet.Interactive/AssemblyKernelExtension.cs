// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.Commands;
using Microsoft.DotNet.Interactive.CSharp;
using Microsoft.Spark.Interop;
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
        private const string TempDirEnvVar = "DOTNET_SPARK_EXTENSION_INTERACTIVE_TMPDIR";
        private const string PreserveTempDirEnvVar = "DOTNET_SPARK_EXTENSION_INTERACTIVE_PRESERVE_TMPDIR";
        private const string CsharpKernelName = "csharp";

        private ReferencedPackagesExtractor _referencedPackagesContainer;
        private PackageResolver _packageResolver;

        /// <summary>
        /// Called by the Microsoft.DotNet.Interactive Assembly Extension Loader.
        /// </summary>
        /// <param name="kernel">The kernel calling this method.</param>
        /// <returns><see cref="Task.CompletedTask"/> when extension is loaded.</returns>
        public Task OnLoadAsync(Kernel kernel)
        {
            if (kernel is CompositeKernel compositeKernel)
            {
                var cSharpKernel = kernel.FindKernelByName(CsharpKernelName) as CSharpKernel;

                _referencedPackagesContainer = new ReferencedPackagesExtractor(cSharpKernel);
                _packageResolver = new PackageResolver(_referencedPackagesContainer);

                Environment.SetEnvironmentVariable(Constants.RunningREPLEnvVar, "true");

                var tempDir = new DirectoryInfo(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
                tempDir.Create();

                kernel.RegisterForDisposal(() =>
                {
                    if (!EnvironmentUtils.GetEnvironmentVariableAsBool(PreserveTempDirEnvVar))
                    {
                        tempDir.Delete(true);
                    }
                });

                compositeKernel.AddMiddleware(async (command, context, next) =>
                {
                    await next(command, context);

                    if ((context.HandlingKernel is CSharpKernel kernel) &&
                        (command is SubmitCode) &&
                        TryGetSparkSession(out SparkSession sparkSession) &&
                        TryEmitAssembly(kernel, tempDir.FullName, out string assemblyPath))
                    {
                        sparkSession.SparkContext.AddFile(assemblyPath);

                        foreach (string filePath in GetPackageFiles(tempDir.FullName))
                        {
                            sparkSession.SparkContext.AddFile(filePath);
                        }
                    }
                });
            }

            return Task.CompletedTask;
        }

        private DirectoryInfo CreateTempDirectory()
        {
            string envTempDir = Environment.GetEnvironmentVariable(TempDirEnvVar);
            string tempDirBasePath = string.IsNullOrEmpty(envTempDir) ?
                Directory.GetCurrentDirectory() :
                envTempDir;

            if (!IsPathValid(tempDirBasePath))
            {
                throw new Exception($"[{GetType().Name}] Spaces in " +
                    $"'{tempDirBasePath}' is unsupported. Set the {TempDirEnvVar} " +
                    "environment variable to control the base path. Please see " +
                    "https://issues.apache.org/jira/browse/SPARK-30126 and " +
                    "https://github.com/apache/spark/pull/26773 for more details.");
            }

            return Directory.CreateDirectory(
                Path.Combine(tempDirBasePath, Path.GetRandomFileName()));
        }

        private bool TryEmitAssembly(CSharpKernel kernel, string dstPath, out string assemblyPath)
        {
            Compilation compilation = kernel.ScriptState.Script.GetCompilation();
            string assemblyName =
                AssemblyLoader.NormalizeAssemblyName(compilation.AssemblyName);
            assemblyPath = Path.Combine(dstPath, $"{assemblyName}.dll");
            if (!File.Exists(assemblyPath))
            {
                FileSystemExtensions.Emit(compilation, assemblyPath);
                return true;
            }

            throw new Exception(
                $"TryEmitAssembly() unexpected duplicate assembly: ${assemblyPath}");
        }

        /// <summary>
        /// Uses <see cref="SparkSession.GetActiveSession"/> to retrieve active or default session if one exists
        /// Otherwise returns  <see cref="SparkSession.GetDefaultSession"/>
        /// </summary>
        /// <param name="sparkSession">Out variable, that is set to active Spark Session if one found</param>
        /// <returns>True if spark session is found, otherwise False.</returns>
        private bool TryGetSparkSession(out SparkSession sparkSession)
        {
            sparkSession = default;

            try
            {
                sparkSession = SparkSession.GetActiveSession() ?? SparkSession.GetDefaultSession();
                return sparkSession != null;
            }
            catch (Exception ex) when (ex.InnerException is JvmException)
            {
                return false;
            }
        }

        private IEnumerable<string> GetPackageFiles(string path)
        {
            foreach (string filePath in _packageResolver.GetFiles(path))
            {
                if (IsPathValid(filePath))
                {
                    yield return filePath;
                }
                else
                {
                    // Copy file to a path without spaces.
                    string fileDestPath = Path.Combine(
                        path,
                        Path.GetFileName(filePath).Replace(" ", string.Empty));
                    File.Copy(filePath, fileDestPath);
                    yield return fileDestPath;
                }
            }
        }

        /// <summary>
        /// In some versions of Spark, spaces is unsupported when using
        /// <see cref="SparkContext.AddFile(string, bool)"/>.
        /// 
        /// For more details please see:
        /// - https://issues.apache.org/jira/browse/SPARK-30126
        /// - https://github.com/apache/spark/pull/26773
        /// </summary>
        /// <param name="path">The path to validate.</param>
        /// <returns>true if the path is supported by Spark, false otherwise.</returns>
        private bool IsPathValid(string path)
        {
            if (!path.Contains(" "))
            {
                return true;
            }

            Version version = SparkEnvironment.SparkVersion;
            return version.Major switch
            {
                2 => false,
                3 => true,
                _ => throw new NotSupportedException($"Spark {version} not supported.")
            };
        }
    }
}
