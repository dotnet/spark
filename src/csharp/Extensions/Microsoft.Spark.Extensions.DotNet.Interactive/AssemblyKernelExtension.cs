using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.CSharp;
using Microsoft.DotNet.Interactive.Utility;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    public class AssemblyKernelExtension : IKernelExtension
    {
        public static bool Enabled { get; set; } = true;

        public Task OnLoadAsync(IKernel kernel)
        {
            if (kernel is CSharpKernel kernelBase)
            {
                string home = Environment.GetEnvironmentVariable("HOME");
                DirectoryInfo tempDir = Directory.CreateDirectory(
                    Path.Combine(
                        string.IsNullOrEmpty(home) ? Directory.GetCurrentDirectory() : home,
                        Path.GetRandomFileName()));
                var disposableDirectory = new DisposableDirectory(tempDir);

                kernelBase.AddMiddleware(async (command, context, next) =>
                {
                    if (!Enabled)
                    {
                        return;
                    }

                    var kernel = context.HandlingKernel as CSharpKernel;
                    Compilation preCellCompilation = kernel.ScriptState.Script.GetCompilation();

                    string assemblyName =
                        AssemblyLoader.NormalizeAssemblyName(preCellCompilation.AssemblyName);
                    string assemblyPath = Path.Combine(tempDir.FullName, $"{assemblyName}.dll");
                    if (!File.Exists(assemblyPath))
                    {
                        FileSystemExtensions.Emit(preCellCompilation, assemblyPath);
                        SparkSession.Active().SparkContext.AddFile(assemblyPath);
                    }

                    PackagesHelper.GenerateAndAddFiles(
                        tempDir,
                        SparkSession.Active().SparkContext.AddFile);

                    await next(command, context);
                });

                kernelBase.RegisterForDisposal(disposableDirectory);
            }

            return Task.CompletedTask;
        }
    }
}
