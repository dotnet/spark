// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Xunit;

namespace Microsoft.Spark.E2ETest
{
    public class SparkFixtureConfigurationTests
    {
        [Fact]
        public void GetPackageResolutionOptionsUsesCommunityDefault()
        {
            Assert.Equal(
                "--repositories https://repos.spark-packages.org/",
                SparkFixture.GetPackageResolutionOptions(null, null));
        }

        [Theory]
        [InlineData(
            "file:///tmp/ivysettings.xml",
            "--conf spark.jars.ivySettings=file:///tmp/ivysettings.xml " +
                "--repositories https://pkgs.dev.azure.com/example/maven/v1")]
        [InlineData(
            @"D:\a\_temp\ivysettings.xml",
            @"--conf spark.jars.ivySettings=D:\a\_temp\ivysettings.xml " +
                "--repositories https://pkgs.dev.azure.com/example/maven/v1")]
        public void GetPackageResolutionOptionsUsesCustomIvySettingsAndRepository(
            string ivySettings,
            string expected)
        {
            Assert.Equal(
                expected,
                SparkFixture.GetPackageResolutionOptions(
                    ivySettings,
                    "https://pkgs.dev.azure.com/example/maven/v1"));
        }

        [Theory]
        [InlineData("file:///tmp/ivysettings.xml", null)]
        [InlineData(null, "https://pkgs.dev.azure.com/example/maven/v1")]
        public void GetPackageResolutionOptionsRejectsPartialConfiguration(
            string ivySettings,
            string repositories)
        {
            Assert.Throws<InvalidOperationException>(
                () => SparkFixture.GetPackageResolutionOptions(ivySettings, repositories));
        }
    }
}
