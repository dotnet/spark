// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.Commands;
using Microsoft.DotNet.Interactive.CSharp;
using Microsoft.DotNet.Interactive.Events;
using Microsoft.Spark.E2ETest;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.Extensions.DotNet.Interactive.E2ETest
{
    public class InteractiveTests
    {
        [Fact]
        public async Task TestCrossCellUdfAndRecovery()
        {
            string previousRepl = Environment.GetEnvironmentVariable("DOTNET_SPARK_RUNNING_REPL");
            string previousRunMode = Environment.GetEnvironmentVariable("SPARK_NET_RUN_MODE");
            try
            {
                // JvmBridge caches the REPL mode at construction. This project runs
                // in a separate testhost, before any ordinary SQL fixture is created.
                Environment.SetEnvironmentVariable("DOTNET_SPARK_RUNNING_REPL", "true");
                Environment.SetEnvironmentVariable("SPARK_NET_RUN_MODE", "N");
                using var fixture = new SparkFixture();
                using var kernel = new CompositeKernel();
                var csharp = new CSharpKernel();
                csharp.AddAssemblyReferences(new[] { typeof(SparkSession).Assembly.Location });
                kernel.Add(csharp);
                await new AssemblyKernelExtension().OnLoadAsync(kernel);

                await SubmitAsync(kernel, @"
                    using System;
                    using System.Linq;
                    using Microsoft.Spark.Sql;
                    using static Microsoft.Spark.Sql.Functions;
                    public class CellOffset
                    {
                        public long Value;
                        public long Apply(long value) => value + Value;
                    }
                    var add = Udf<long, long>(new CellOffset { Value = 7 }.Apply);
                ");
                await SubmitAsync(kernel, @"
                    var spark = SparkSession.Builder().GetOrCreate();
                    var values = spark.Range(0, 3, 1, 2).Select(add(Col(""id"")))
                        .Collect().Select(row => row.GetAs<long>(0)).OrderBy(value => value).ToArray();
                ");
                AssertValues(csharp, "values", 7, 8, 9);

                await SubmitAsync(kernel, @"
                    public class LaterCell
                    {
                        public static long Apply(long value) => value * 3;
                        public static long Fail(long value) => throw new InvalidOperationException(""wi10-worker-failure"");
                    }
                    var multiply = Udf<long, long>(LaterCell.Apply);
                    var fail = Udf<long, long>(LaterCell.Fail);
                ");
                await SubmitAsync(kernel, @"
                    var later = spark.Range(0, 3, 1, 2).Select(multiply(Col(""id"")))
                        .Collect().Select(row => row.GetAs<long>(0)).OrderBy(value => value).ToArray();
                ");
                AssertValues(csharp, "later", 0, 3, 6);

                KernelCommandResult failedAction = await kernel.SendAsync(new SubmitCode(@"
                    public class FailedCell
                    {
                        public static long Apply(long value) => value + 100;
                    }
                    var fromFailedCell = Udf<long, long>(FailedCell.Apply);
                    spark.Range(1).Select(fail(Col(""id""))).Collect().ToArray();
                ", "csharp"));
                CommandFailed workerFailure = Assert.Single(failedAction.Events.OfType<CommandFailed>());
                Assert.NotNull(workerFailure.Exception);
                Assert.Contains("wi10-worker-failure", workerFailure.Exception.ToString());

                await SubmitAsync(kernel, @"
                    var recovered = spark.Range(0, 3, 1, 2).Select(fromFailedCell(Col(""id"")))
                        .Collect().Select(row => row.GetAs<long>(0)).OrderBy(value => value).ToArray();
                ");
                AssertValues(csharp, "recovered", 100, 101, 102);

                KernelCommandResult compileError = await kernel.SendAsync(new SubmitCode("int broken = ;", "csharp"));
                CommandFailed compilerFailure = Assert.Single(compileError.Events.OfType<CommandFailed>());
                Assert.Contains("CS1525", compilerFailure.Message);
                Assert.DoesNotContain("duplicate assembly", compilerFailure.Message);

                await SubmitAsync(kernel, @"
                    var afterCompileError = spark.Range(0, 3, 1, 2).Select(multiply(Col(""id"")))
                        .Collect().Select(row => row.GetAs<long>(0)).OrderBy(value => value).ToArray();
                ");
                AssertValues(csharp, "afterCompileError", 0, 3, 6);
            }
            finally
            {
                Environment.SetEnvironmentVariable("SPARK_NET_RUN_MODE", previousRunMode);
                Environment.SetEnvironmentVariable("DOTNET_SPARK_RUNNING_REPL", previousRepl);
            }
        }

        private static async Task SubmitAsync(CompositeKernel kernel, string code)
        {
            KernelCommandResult result = await kernel.SendAsync(new SubmitCode(code, "csharp"));
            string[] failures = result.Events.OfType<CommandFailed>().Select(failure => failure.Message).ToArray();
            Assert.True(failures.Length == 0, string.Join(Environment.NewLine, failures));
            Assert.Single(result.Events.OfType<CommandSucceeded>());
        }

        private static void AssertValues(CSharpKernel kernel, string name, params long[] expected)
        {
            Assert.True(kernel.TryGetValue(name, out long[] values));
            Assert.Equal(expected, values);
        }
    }
}
