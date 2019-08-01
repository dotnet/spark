// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Microsoft.Spark.Services
{
    /// <summary>
    /// Implementation of configuration service that helps getting config settings
    /// to be used in .NET backend.
    /// </summary>
    internal sealed class ConfigurationService : IConfigurationService
    {
        public const string WorkerDirEnvVarName = "DOTNET_WORKER_DIR";
        public const string WorkerReadBufferSizeEnvVarName = "spark.dotnet.worker.readBufferSize";
        public const string WorkerWriteBufferSizeEnvVarName =
            "spark.dotnet.worker.writeBufferSize";

        private const string DotnetBackendPortEnvVarName = "DOTNETBACKEND_PORT";
        private const int DotnetBackendDebugPort = 5567;

        private const string DotNetApplicationArchiveEnvVarName = "DOTNET_APPLICATION_ARCHIVE";
        private string _applicationArchive;

        private static readonly string s_procBaseFileName = "Microsoft.Spark.Worker";
        private static readonly string s_procFileName =
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ?
            $"{s_procBaseFileName}.exe" :
            s_procBaseFileName;

        private readonly ILoggerService _logger =
            LoggerServiceFactory.GetLogger(typeof(ConfigurationService));

        private string _workerPath;

        /// <summary>
        /// Returns the port number for socket communication between JVM and CLR.
        /// </summary>
        public int GetBackendPortNumber()
        {
            if (!int.TryParse(
                Environment.GetEnvironmentVariable(DotnetBackendPortEnvVarName),
                out int portNumber))
            {
                _logger.LogInfo($"'{DotnetBackendPortEnvVarName}' environment variable is not set.");
                portNumber = DotnetBackendDebugPort;
            }

            _logger.LogInfo($"Using port {portNumber} for connection.");

            return portNumber;
        }

        /// <summary>
        /// Returns the worker executable path.
        /// </summary>
        /// <returns>Worker executable path</returns>
        public string GetWorkerExePath()
        {
            if (_workerPath != null)
            {
                return _workerPath;
            }

            string workerDir = Environment.GetEnvironmentVariable(WorkerDirEnvVarName);

            // If the WorkerDirEnvName environment variable is set, the worker path is constructed
            // based on it.
            if (!string.IsNullOrEmpty(workerDir))
            {
                _workerPath = Path.Combine(workerDir, s_procFileName);
                _logger.LogDebug($"Using the environment variable to construct .NET worker path: {_workerPath}.");
                return _workerPath;
            }

            // Otherwise, the worker exectuable name is returned meaning it should be PATH.
            _workerPath = s_procFileName;
            return _workerPath;
        }

        public string GetApplicationArchiveName()
        {
            if(_applicationArchive != null)
            {
                return _applicationArchive;
            }

            string appArchive = Environment.GetEnvironmentVariable(DotNetApplicationArchiveEnvVarName);

            if(!string.IsNullOrEmpty(appArchive))
            {
                _logger.LogInfo("Application archive variable not set.");
                return null;
            }

            _logger.LogInfo($"Application archive is {appArchive}.");

            _applicationArchive = appArchive;
            return _applicationArchive;
        }
    }
}
