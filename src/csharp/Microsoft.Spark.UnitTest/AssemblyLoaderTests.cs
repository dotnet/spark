// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Reflection;
using System.Runtime.Loader;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;
using Moq;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class AssemblyLoaderTests
    {
        private readonly Mock<IJvmBridge> _mockJvm;

        public AssemblyLoaderTests(SparkFixture _fixture)
        {
            _mockJvm = _fixture.MockJvm;
        }

        [Fact]
        public void TestAssemblySearchPathResolver()
        {
            string sparkFilesDir = SparkFiles.GetRootDirectory();
            string curDir = Directory.GetCurrentDirectory();
            string appDir = AppDomain.CurrentDomain.BaseDirectory;

            // Test the default scenario.
            string[] searchPaths = AssemblySearchPathResolver.GetAssemblySearchPaths();
            Assert.Equal(new[] { sparkFilesDir, curDir, appDir }, searchPaths);

            // Test the case where DOTNET_ASSEMBLY_SEARCH_PATHS is defined.
            char sep = Path.PathSeparator;
            Environment.SetEnvironmentVariable(
                AssemblySearchPathResolver.AssemblySearchPathsEnvVarName,
                $"mydir1, mydir2, .{sep}mydir3,.{sep}mydir4");

            searchPaths = AssemblySearchPathResolver.GetAssemblySearchPaths();
            Assert.Equal(
                new[] {
                    "mydir1",
                    "mydir2",
                    Path.Combine(curDir, $".{sep}mydir3"),
                    Path.Combine(curDir, $".{sep}mydir4"),
                    sparkFilesDir,
                    curDir,
                    appDir },
                searchPaths);

            Environment.SetEnvironmentVariable(
                AssemblySearchPathResolver.AssemblySearchPathsEnvVarName,
                null);
        }

        [Fact]
        public void TestResolveAssemblyWithRelativePath()
        {
            _mockJvm.Setup(m => m.CallStaticJavaMethod(
                "org.apache.spark.SparkFiles",
                "getRootDirectory"))
                .Returns(".");

            AssemblyLoader.LoadFromFile = AssemblyLoadContext.Default.LoadFromAssemblyPath;
            Assembly expectedAssembly = Assembly.GetExecutingAssembly();
            Assembly actualAssembly = AssemblyLoader.ResolveAssembly(expectedAssembly.FullName);

            Assert.Equal(expectedAssembly, actualAssembly);
        }
    }
}
