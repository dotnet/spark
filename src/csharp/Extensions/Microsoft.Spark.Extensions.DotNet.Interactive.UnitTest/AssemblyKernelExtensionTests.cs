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
using Xunit;

namespace Microsoft.Spark.Extensions.DotNet.Interactive.UnitTest
{
    public class AssemblyKernelExtensionTests
    {
        [Theory]
        [InlineData("3.0.0")]
        [InlineData("3.5.3")]
        [InlineData("4.0.0")]
        [InlineData("4.0.1")]
        [InlineData("4.0.2")]
        [InlineData("4.0.3")]
        [InlineData("4.0.4")]
        public void TestPathsWithSpaces(string version)
        {
            AssemblyKernelExtension.ValidatePath("packages with spaces/package.nupkg", new Version(version));
        }

        [Theory]
        [InlineData("2.4.8")]
        [InlineData("4.1.0")]
        [InlineData("5.0.0")]
        public void TestUnsupportedPathsWithSpaces(string version)
        {
            Assert.Throws<NotSupportedException>(() =>
                AssemblyKernelExtension.ValidatePath("packages with spaces/package.nupkg", new Version(version)));

            AssemblyKernelExtension.ValidatePath("packages/package.nupkg", new Version(version));
        }

        [Fact]
        public void TestKernelWithoutNuGetProvider()
        {
            using var kernel = new CSharpKernel();
            Assert.Empty(new ReferencedPackagesExtractor(kernel).ResolvedPackageReferences);
        }

        [Fact]
        public void TestMissingKernelIsNotTreatedAsAnEmptyPackageList()
        {
            Assert.ThrowsAny<Exception>(() =>
                new ReferencedPackagesExtractor(null).ResolvedPackageReferences.ToArray());
        }

        [Fact]
        public async Task TestKernelWithoutCSharpStillHandlesCommands()
        {
            string previousMode = Environment.GetEnvironmentVariable("DOTNET_SPARK_RUNNING_REPL");
            try
            {
                using var kernel = new CompositeKernel { new NonCSharpKernel() };
                await new AssemblyKernelExtension().OnLoadAsync(kernel);
                KernelCommandResult result = await kernel.SendAsync(new SubmitCode("hello", "other"));
                Assert.Empty(result.Events.OfType<CommandFailed>());
                Assert.Single(result.Events.OfType<CommandSucceeded>());
            }
            finally
            {
                Environment.SetEnvironmentVariable("DOTNET_SPARK_RUNNING_REPL", previousMode);
            }
        }

        [Fact]
        public async Task TestInitialCompilationFailureDoesNotPublishAnAssembly()
        {
            string previousMode = Environment.GetEnvironmentVariable("DOTNET_SPARK_RUNNING_REPL");
            try
            {
                using var kernel = new CompositeKernel { new CSharpKernel() };
                await new AssemblyKernelExtension().OnLoadAsync(kernel);
                KernelCommandResult result = await kernel.SendAsync(new SubmitCode("int broken = ;", "csharp"));
                CommandFailed failure = Assert.Single(result.Events.OfType<CommandFailed>());
                Assert.Contains("CS1525", failure.Message);
            }
            finally
            {
                Environment.SetEnvironmentVariable("DOTNET_SPARK_RUNNING_REPL", previousMode);
            }
        }

        private sealed class NonCSharpKernel : Kernel
        {
            public NonCSharpKernel() : base("other")
            {
                RegisterCommandHandler<SubmitCode>((command, context) => Task.CompletedTask);
            }
        }
    }
}
