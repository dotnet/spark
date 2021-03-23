// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Services;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class ConfigurationServiceTests : IDisposable
    {
        private readonly WorkerDirEnvVars _workerDirEnvVars;

        public ConfigurationServiceTests()
        {
            var version = new Version(AssemblyInfoProvider.MicrosoftSparkAssemblyInfo().AssemblyVersion);
            _workerDirEnvVars = new WorkerDirEnvVars
            {
                WorkerDir = new EnvVar {
                    Name = ConfigurationService.DefaultWorkerDirEnvVarName,
                    Value = Environment.GetEnvironmentVariable(ConfigurationService.DefaultWorkerDirEnvVarName)
                },
                WorkerMajorMinorBuildDir = new EnvVar
                {
                    Name = string.Format(
                        ConfigurationService.WorkerVerDirEnvVarNameFormat,
                        $"{version.Major}_{version.Minor}_{version.Build}"),
                    Value = Environment.GetEnvironmentVariable(
                        string.Format(
                            ConfigurationService.WorkerVerDirEnvVarNameFormat,
                            $"{version.Major}_{version.Minor}_{version.Build}"))
                },
                WorkerMajorMinorDir = new EnvVar
                {
                    Name = string.Format(
                        ConfigurationService.WorkerVerDirEnvVarNameFormat,
                        $"{version.Major}_{version.Minor}"),
                    Value = Environment.GetEnvironmentVariable(
                        string.Format(
                            ConfigurationService.WorkerVerDirEnvVarNameFormat,
                            $"{version.Major}_{version.Minor}"))
                },
                WorkerMajorDir = new EnvVar
                {
                    Name = string.Format(
                        ConfigurationService.WorkerVerDirEnvVarNameFormat, version.Major),
                    Value = Environment.GetEnvironmentVariable(
                        string.Format(
                            ConfigurationService.WorkerVerDirEnvVarNameFormat, version.Major))
                }
            };

            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerDir.Name, "");
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name, "");
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorDir.Name, "");
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name, "");
        }

        [Fact]
        public void TestWorkerEnvName()
        {
            {
                var configService = new ConfigurationService();

                Assert.True(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(
                            _workerDirEnvVars.WorkerMajorMinorBuildDir.Name)));
                Assert.True(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorDir.Name)));
                Assert.True(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name)));
                Assert.Equal(
                    ConfigurationService.DefaultWorkerDirEnvVarName, configService.WorkerDirEnvVarName);
            }

            {
                var configService = new ConfigurationService();
                Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name, "workerMajorDir");

                Assert.True(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(
                            _workerDirEnvVars.WorkerMajorMinorBuildDir.Name)));
                Assert.True(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorDir.Name)));
                Assert.False(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name)));
                Assert.Equal(_workerDirEnvVars.WorkerMajorDir.Name, configService.WorkerDirEnvVarName);
            }

            {
                var configService = new ConfigurationService();
                Environment.SetEnvironmentVariable(
                    _workerDirEnvVars.WorkerMajorMinorDir.Name, "workerMajorMinorDir");

                Assert.True(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(
                            _workerDirEnvVars.WorkerMajorMinorBuildDir.Name)));
                Assert.False(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorDir.Name)));
                Assert.False(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name)));
                Assert.Equal(_workerDirEnvVars.WorkerMajorMinorDir.Name, configService.WorkerDirEnvVarName);
            }

            {
                var configService = new ConfigurationService();
                Environment.SetEnvironmentVariable(
                    _workerDirEnvVars.WorkerMajorMinorBuildDir.Name, "workerMajorMinorBuildDir");
                Assert.False(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name)));
                Assert.False(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorDir.Name)));
                Assert.False(
                    string.IsNullOrWhiteSpace(
                        Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name)));
                Assert.Equal(
                    _workerDirEnvVars.WorkerMajorMinorBuildDir.Name, configService.WorkerDirEnvVarName);
            }
        }

        [Fact]
        public void TestWorkerExePathWithNoEnvVars()
        {
            var configService = new ConfigurationService();

            Assert.True(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name)));
            Assert.True(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorDir.Name)));
            Assert.True(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name)));
            Assert.True(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerDir.Name)));

            // Environment variables not set, only Microsoft.Spark.Worker filename should be returned.
            Assert.Equal(ConfigurationService.ProcFileName, configService.GetWorkerExePath());
        }

        [Fact]
        public void TestWorkerExePathWithWorkerDirEnvVar()
        {
            var configService = new ConfigurationService();
            var workerDir = "workerDir";
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerDir.Name, workerDir);

            Assert.True(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name)));
            Assert.True(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorDir.Name)));
            Assert.True(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name)));
            Assert.False(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerDir.Name)));

            // Only ConfigurationService.WorkerDirEnvVarName is set, WorkerExePath will be built using it.
            Assert.Equal(
                Path.Combine(workerDir, ConfigurationService.ProcFileName),
                configService.GetWorkerExePath());
        }

        [Fact]
        public void TestWorkerExePathWithEnvVarPrecedence()
        {
            var configService = new ConfigurationService();
            var workerDir = "workerDir";
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerDir.Name, workerDir);
            var workerMajorDir = "workerMajorDir";
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name, workerMajorDir);

            Assert.True(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name)));
            Assert.True(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorDir.Name)));
            Assert.False(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name)));
            Assert.False(
                string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(_workerDirEnvVars.WorkerDir.Name)));


            // DOTNET_WORKER_DIR and DOTNET_WORKER_{MAJOR}_DIR environment variables are set.
            // Ensure that the environment variable constructed with the assembly version takes
            // precedence.
            Assert.Equal(
                Path.Combine(workerMajorDir, ConfigurationService.ProcFileName),
                configService.GetWorkerExePath());
        }

        public void Dispose()
        {
            Environment.SetEnvironmentVariable(
                _workerDirEnvVars.WorkerDir.Name,
                _workerDirEnvVars.WorkerDir.Value);
            Environment.SetEnvironmentVariable(
                _workerDirEnvVars.WorkerMajorMinorBuildDir.Name,
                _workerDirEnvVars.WorkerMajorMinorBuildDir.Value);
            Environment.SetEnvironmentVariable(
                _workerDirEnvVars.WorkerMajorMinorDir.Name,
                _workerDirEnvVars.WorkerMajorMinorDir.Value);
            Environment.SetEnvironmentVariable(
                _workerDirEnvVars.WorkerMajorDir.Name,
                _workerDirEnvVars.WorkerMajorDir.Value);
        }

        private class WorkerDirEnvVars
        {
            public EnvVar WorkerDir { get; set; }
            public EnvVar WorkerMajorMinorBuildDir { get; set; }
            public EnvVar WorkerMajorMinorDir { get; set; }
            public EnvVar WorkerMajorDir { get; set; }
        }

        private class EnvVar
        {
            public string Name { get; set; }
            public string Value { get; set; }
        }
    }
}
