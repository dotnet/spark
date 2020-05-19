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
    /// Adds nuget and assembly dependencies to the active <see cref="SparkSession"/>
    /// using <see cref="SparkContext.AddFile(string, bool)"/>.
    /// </summary>
    public class AssemblyKernelExtension : IKernelExtension
    {
        /// <summary>
        /// Called by the Microsoft.DotNet.Interactive Assembly Extension Loader.
        /// </summary>
        /// <param name="kernel">The kernel calling this method.</param>
        public Task OnLoadAsync(IKernel kernel)
        {
            if (kernel is CompositeKernel kernelBase)
            {
                string home = Environment.GetEnvironmentVariable("HOME");
                DirectoryInfo tempDir = Directory.CreateDirectory(
                    Path.Combine(
                        string.IsNullOrEmpty(home) ? Directory.GetCurrentDirectory() : home,
                        Path.GetRandomFileName()));

                kernelBase.RegisterForDisposal(new DisposableDirectory(tempDir));

                kernelBase.AddMiddleware(async (command, context, next) =>
                {
                    if (command is SubmitCode)
                    {
                        var kernel = context.HandlingKernel as CSharpKernel;
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
                            tempDir,
                            s => SparkSession.Active().SparkContext.AddFile(s, false));
                    }

                    await next(command, context);
                });
            }

            return Task.CompletedTask;
        }
    }
}
