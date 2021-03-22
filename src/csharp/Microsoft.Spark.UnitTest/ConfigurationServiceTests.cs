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
        private readonly string _workerDir =
            Environment.GetEnvironmentVariable(ConfigurationService.WorkerDirEnvVarName);
        private readonly string _workerVerDir =
            Environment.GetEnvironmentVariable(ConfigurationService.WorkerVerDirEnvVarName);

        public ConfigurationServiceTests()
        {
            Environment.SetEnvironmentVariable(ConfigurationService.WorkerDirEnvVarName, "");
            Environment.SetEnvironmentVariable(ConfigurationService.WorkerVerDirEnvVarName, "");
        }

        [Fact]
        public void TestWorkerVerEnvName()
        {
            string expected = string.Format(
                ConfigurationService.WorkerVerDirEnvVarNameFormat,
                new Version(AssemblyInfoProvider.MicrosoftSparkAssemblyInfo().AssemblyVersion).Major);
            Assert.Equal(expected, ConfigurationService.WorkerVerDirEnvVarName);
        }

        [Fact]
        public void TestWorkerExePathWithNoEnvVars()
        {
            var configService = new ConfigurationService();
            // Environment variables not set, only Microsoft.Spark.Worker filename should be returned.
            Assert.Equal(ConfigurationService.ProcFileName, configService.GetWorkerExePath());
        }

        [Fact]
        public void TestWorkerExePathWithWorkerDirEnvVar()
        {
            var configService = new ConfigurationService();
            var workerDir = "workerDir";
            Environment.SetEnvironmentVariable(ConfigurationService.WorkerDirEnvVarName, workerDir);

            // Only ConfigurationService.WorkerDirEnvVarName is set, WorkerExePath will be built using it.
            Assert.Equal(Path.Combine(workerDir, ConfigurationService.ProcFileName), configService.GetWorkerExePath());
        }

        [Fact]
        public void TestWorkerExePathWithEnvVarPrecedence()
        {
            var configService = new ConfigurationService();
            var workerDir = "workerDir";
            Environment.SetEnvironmentVariable(ConfigurationService.WorkerDirEnvVarName, workerDir);
            var workerVerDir = "workerVerDir";
            Environment.SetEnvironmentVariable(ConfigurationService.WorkerVerDirEnvVarName, workerVerDir);

            // ConfigurationService.WorkerDirEnvVarName and ConfigurationService.WorkerVerDirEnvVarName
            // environment variables are set. ConfigurationService.WorkerVerDirEnvVarName will take
            // precedence.
            Assert.Equal(Path.Combine(workerVerDir, ConfigurationService.ProcFileName), configService.GetWorkerExePath());
        }

        public void Dispose()
        {
            if (!string.IsNullOrWhiteSpace(_workerDir))
            {
                Environment.SetEnvironmentVariable(ConfigurationService.WorkerDirEnvVarName, _workerDir);
            }

            if (!string.IsNullOrWhiteSpace(_workerVerDir))
            {
                Environment.SetEnvironmentVariable(
                    ConfigurationService.WorkerVerDirEnvVarName, _workerVerDir);
            }
        }
    }
}
