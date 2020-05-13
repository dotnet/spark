using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.CSharp;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    public class AssemblyKernelExtension : IKernelExtension
    {
        public Task OnLoadAsync(IKernel kernel)
        {
            if (kernel is CSharpKernel kernelBase)
            {
                kernelBase.AddMiddleware(async (command, context, next) =>
                {
                    var kernel = context.HandlingKernel as CSharpKernel;
                    Compilation preCellCompilation = kernel.ScriptState.Script.GetCompilation();

                    string path = Environment.GetEnvironmentVariable("HOME");
                    string assemblyName =
                        AssemblyLoader.NormalizeAssemblyName(preCellCompilation.AssemblyName);
                    string assemblyPath =
                        Path.Combine(
                            string.IsNullOrEmpty(path) ? Directory.GetCurrentDirectory() : path,
                            $"{assemblyName}.dll");
                    Directory.CreateDirectory(path);
                    if (!File.Exists(assemblyPath))
                    {
                        FileSystemExtensions.Emit(preCellCompilation, assemblyPath);
                        SparkSession.Active().SparkContext.AddFile(assemblyPath);
                    }

                    PackagesHelper.DoWork(path, SparkSession.Active().SparkContext.AddFile);

                    await next(command, context);
                });
            }

            return Task.CompletedTask;
        }
    }
}
