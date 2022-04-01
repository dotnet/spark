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
                WorkerDir = new EnvVar(ConfigurationService.DefaultWorkerDirEnvVarName),
                WorkerMajorMinorBuildDir = new EnvVar(
                    string.Format(
                        ConfigurationService.WorkerVerDirEnvVarNameFormat,
                        $"{version.Major}_{version.Minor}_{version.Build}")),
                WorkerMajorMinorDir = new EnvVar(
                    string.Format(
                        ConfigurationService.WorkerVerDirEnvVarNameFormat,
                        $"{version.Major}_{version.Minor}")),
                WorkerMajorDir = new EnvVar(
                    string.Format(ConfigurationService.WorkerVerDirEnvVarNameFormat, version.Major))
            };

            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerDir.Name, null);
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name, null);
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerMajorMinorDir.Name, null);
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name, null);
        }

        [Fact]
        public void TestWorkerExePathWithNoEnvVars()
        {
            var configService = new ConfigurationService();

            Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name));
            Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorDir.Name));
            Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorDir.Name));
            Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerDir.Name));

            // Environment variables not set, only Microsoft.Spark.Worker filename should be returned.
            Assert.Equal(ConfigurationService.ProcFileName, configService.GetWorkerExePath());
        }

        [Fact]
        public void TestWorkerExePathWithWorkerDirEnvVar()
        {
            var configService = new ConfigurationService();
            string workerDir = "workerDir";
            Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerDir.Name, workerDir);

            Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name));
            Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorDir.Name));
            Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorDir.Name));
            Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerDir.Name));

            // Only WorkerDir is set, WorkerExePath will be built using it.
            Assert.Equal(
                Path.Combine(workerDir, ConfigurationService.ProcFileName),
                configService.GetWorkerExePath());
        }

        [Fact]
        public void TestWorkerExePathWithEnvVarPrecedence()
        {
            {
                var configService = new ConfigurationService();
                string workerDir = "workerDir";
                Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerDir.Name, workerDir);

                Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name));
                Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorDir.Name));
                Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorDir.Name));
                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerDir.Name));
                Assert.Equal(
                    Path.Combine(workerDir, ConfigurationService.ProcFileName),
                    configService.GetWorkerExePath());
            }

            {
                var configService = new ConfigurationService();
                string workerMajorDir = "workerMajorDir";
                Environment.SetEnvironmentVariable(_workerDirEnvVars.WorkerMajorDir.Name, workerMajorDir);

                Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name));
                Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorDir.Name));
                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerMajorDir.Name));
                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerDir.Name));
                Assert.Equal(
                    Path.Combine(workerMajorDir, ConfigurationService.ProcFileName),
                    configService.GetWorkerExePath());
            }

            {
                var configService = new ConfigurationService();
                string workerMajorMinorDir = "workerMajorMinorDir";
                Environment.SetEnvironmentVariable(
                    _workerDirEnvVars.WorkerMajorMinorDir.Name, workerMajorMinorDir);

                Assert.False(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name));
                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorDir.Name));
                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerMajorDir.Name));
                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerDir.Name));
                Assert.Equal(
                    Path.Combine(workerMajorMinorDir, ConfigurationService.ProcFileName),
                    configService.GetWorkerExePath());
            }

            {
                var configService = new ConfigurationService();
                string workerMajorMinorBuildDir = "workerMajorMinorBuildDir";
                Environment.SetEnvironmentVariable(
                    _workerDirEnvVars.WorkerMajorMinorBuildDir.Name, workerMajorMinorBuildDir);

                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorBuildDir.Name));
                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerMajorMinorDir.Name));
                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerMajorDir.Name));
                Assert.True(IsEnvVarSet(_workerDirEnvVars.WorkerDir.Name));
                Assert.Equal(
                    Path.Combine(workerMajorMinorBuildDir, ConfigurationService.ProcFileName),
                    configService.GetWorkerExePath());
            }
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

        public bool IsEnvVarSet(string name) =>
            !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name));

        private class WorkerDirEnvVars
        {
            public EnvVar WorkerDir { get; set; }
            public EnvVar WorkerMajorMinorBuildDir { get; set; }
            public EnvVar WorkerMajorMinorDir { get; set; }
            public EnvVar WorkerMajorDir { get; set; }
        }

        private class EnvVar
        {
            public string Name { get; }
            public string Value { get; }

            public EnvVar(string name)
            {
                Name = name;
                Value = Environment.GetEnvironmentVariable(name);
            }

        }
    }
}
