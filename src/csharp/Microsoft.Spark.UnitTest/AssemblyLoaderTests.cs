﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class AssemblyLoaderTests
    {
        [Fact]
        public void TestAssemblySearchPathResolver()
        {
            string curDir = Directory.GetCurrentDirectory();
            string appDir = AppDomain.CurrentDomain.BaseDirectory;

            // Test the default scenario.
            string[] searchPaths = AssemblySearchPathResolver.GetAssemblySearchPaths();
            Assert.Equal(new[] { curDir, appDir }, searchPaths);

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
                    curDir,
                    appDir },
                searchPaths);

            Environment.SetEnvironmentVariable(
                AssemblySearchPathResolver.AssemblySearchPathsEnvVarName,
                null);
        }
    }
}
