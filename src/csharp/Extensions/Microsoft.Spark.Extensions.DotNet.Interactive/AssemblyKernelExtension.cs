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
using Microsoft.DotNet.Interactive.Utility;
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

        private readonly PackageResolver _packageResolver =
            new PackageResolver(new PackageRestoreContextWrapper());

        /// <summary>
        /// Called by the Microsoft.DotNet.Interactive Assembly Extension Loader.
        /// </summary>
        /// <param name="kernel">The kernel calling this method.</param>
        /// <returns><see cref="Task.CompletedTask"/> when extension is loaded.</returns>
        public Task OnLoadAsync(IKernel kernel)
        {
            if (kernel is CompositeKernel kernelBase)
            {
                Environment.SetEnvironmentVariable(Constants.RunningREPLEnvVar, "true");

                DirectoryInfo tempDir = CreateTempDirectory();
                kernelBase.RegisterForDisposal(new DisposableDirectory(tempDir));

                kernelBase.AddMiddleware(async (command, context, next) =>
                {
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

                    await next(command, context);
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

        private bool TryGetSparkSession(out SparkSession sparkSession)
        {
            sparkSession = SparkSession.GetDefaultSession();
            return sparkSession != null;
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
            return (version.Major, version.Minor, version.Build) switch
            {
                (2, _, _) => false,
                (3, 0, _) => true,
                _ => throw new NotSupportedException($"Spark {version} not supported.")
            };
        }
    }
}
