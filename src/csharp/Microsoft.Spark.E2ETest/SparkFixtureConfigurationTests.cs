// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.UnitTest.TestUtils;
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

        [Theory]
        [InlineData(
            "",
            "--packages org.apache.spark:spark-avro_2.13:4.0.4")]
        [InlineData(
            "--conf spark.python.use.daemon=false",
            "--conf spark.python.use.daemon=false " +
                "--packages org.apache.spark:spark-avro_2.13:4.0.4")]
        [InlineData(
            "--conf spark.python.use.daemon=false ",
            "--conf spark.python.use.daemon=false " +
                "--packages org.apache.spark:spark-avro_2.13:4.0.4")]
        [InlineData(
            "--packages example:package:1.0.0",
            "--packages org.apache.spark:spark-avro_2.13:4.0.4,example:package:1.0.0")]
        public void AddPackagesSeparatesOptionsAndPrependsAvroPackage(
            string args,
            string expected)
        {
            Assert.Equal(
                expected,
                SparkFixture.AddPackages(
                    args,
                    "org.apache.spark:spark-avro_2.13:4.0.4"));
        }

        [Theory]
        [InlineData("2.4.8", "2.11")]
        [InlineData("3.5.3", "2.12")]
        [InlineData("4.0.4", "2.13")]
        public void GetScalaBinaryVersionUsesSparkFamily(
            string sparkVersion,
            string expectedScalaVersion)
        {
            Assert.Equal(
                expectedScalaVersion,
                SparkFixture.GetScalaBinaryVersion(new Version(sparkVersion)));
        }

        [Fact]
        public void GetScalaBinaryVersionRejectsUnknownSparkFamily()
        {
            Assert.Throws<NotSupportedException>(
                () => SparkFixture.GetScalaBinaryVersion(new Version("4.1.0")));
        }

        [Theory]
        [InlineData("2.4.8", "org.apache.spark:spark-avro_2.11:2.4.8")]
        [InlineData("3.5.3", "org.apache.spark:spark-avro_2.12:3.5.3")]
        [InlineData("4.0.4", "org.apache.spark:spark-avro_2.13:4.0.4")]
        public void GetAvroPackageUsesSparkScalaBinary(
            string sparkVersion,
            string expectedPackage)
        {
            Assert.Equal(
                expectedPackage,
                SparkFixture.GetAvroPackage(new Version(sparkVersion)));
        }

        [Fact]
        public void GetAvroPackageRejectsUnknownSparkFamily()
        {
            Assert.Throws<NotSupportedException>(
                () => SparkFixture.GetAvroPackage(new Version("4.1.0")));
        }

        [Fact]
        public void GetSingleJarReturnsOnlyMatch()
        {
            using var tempDirectory = new TemporaryDirectory();
            string jarName = "microsoft-spark-4-0_2.13-2.3.1.jar";
            string jarPath = Path.Combine(tempDirectory.Path, jarName);
            File.WriteAllText(jarPath, string.Empty);

            Assert.Equal(
                jarPath,
                SparkFixture.GetSingleJar(tempDirectory.Path, "microsoft-spark-4-0_2.13-*.jar"));
        }

        [Fact]
        public void GetSingleJarRejectsMissingJar()
        {
            using var tempDirectory = new TemporaryDirectory();

            Assert.Throws<FileNotFoundException>(
                () => SparkFixture.GetSingleJar(tempDirectory.Path, "*.jar"));
        }

        [Fact]
        public void GetSingleJarRejectsDuplicateJars()
        {
            using var tempDirectory = new TemporaryDirectory();
            File.WriteAllText(Path.Combine(tempDirectory.Path, "first.jar"), string.Empty);
            File.WriteAllText(Path.Combine(tempDirectory.Path, "second.jar"), string.Empty);

            Assert.Throws<InvalidOperationException>(
                () => SparkFixture.GetSingleJar(tempDirectory.Path, "*.jar"));
        }

        [Fact]
        public void ValidateExpectedSparkVersionAcceptsExactVersion()
        {
            SparkFixture.ValidateExpectedSparkVersion(
                new Version("4.0.4"),
                "4.0.4");
        }

        [Theory]
        [InlineData("4.0.3")]
        [InlineData("4.1.0")]
        public void ValidateExpectedSparkVersionRejectsDifferentVersion(
            string actualVersion)
        {
            Assert.Throws<InvalidOperationException>(
                () => SparkFixture.ValidateExpectedSparkVersion(
                    new Version(actualVersion),
                    "4.0.4"));
        }

        [Fact]
        public void ValidateExpectedSparkVersionRejectsMalformedVersion()
        {
            Assert.Throws<InvalidOperationException>(
                () => SparkFixture.ValidateExpectedSparkVersion(
                    new Version("4.0.4"),
                    "not-a-version"));
        }
    }
}
