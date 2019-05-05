// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Microsoft.Spark.Services
{
    /// <summary>
    /// Implementation of configuration service that helps getting config settings
    /// to be used in .NET backend.
    /// </summary>
    internal sealed class ConfigurationService : IConfigurationService
    {
        public const string WorkerPathSettingKey = "DotnetWorkerPath";
        public const string WorkerReadBufferSizeEnvName = "spark.dotnet.worker.readBufferSize";
        public const string WorkerWriteBufferSizeEnvName = "spark.dotnet.worker.writeBufferSize";

        private readonly string[] SparkMasterEnvName = new string[] { "spark.master", "MASTER" };
        private const string DotnetBackendPortNumberSettingKey = "DotnetBackendPortNumber";
        private const string DotnetBackendPortEnvName = "DOTNETBACKEND_PORT";
        private const int DotnetBackendDebugPort = 5567;

        private static readonly string s_procBaseFileName = "Microsoft.Spark.Worker";
        private static readonly string s_procFileName =
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ?
            $"{s_procBaseFileName}.exe" :
            s_procBaseFileName;

        private readonly ILoggerService _logger =
            LoggerServiceFactory.GetLogger(typeof(ConfigurationService));
        private readonly DefaultConfiguration _configuration;
        private readonly RunMode _runMode = RunMode.UNKNOWN;

        internal ConfigurationService()
        {
            Assembly entryAssembly = Assembly.GetEntryAssembly();
            // entryAssembly can be null if ConfigurationService is instantiated in unit tests.
            if (entryAssembly == null)
            {
                entryAssembly = new StackTrace().GetFrames().Last().GetMethod().Module.Assembly;
            }

            Configuration appConfig = ConfigurationManager.OpenExeConfiguration(
                entryAssembly.Location);

            // SPARK_MASTER is set by when the driver runs on the Scala side.
            // Depending on the job submission, there are different environment 
            // variables that are set to indicate Spark Master URI:
            // - spark.master for job submissions through spark-submit
            // - MASTER for job submissions through Databricks (Create Job -> Set JAR)
            string sparkMaster = null;
            foreach(string sparkMasterEnv in SparkMasterEnvName)
            {
                sparkMaster = Environment.GetEnvironmentVariable(sparkMasterEnv);
                if (sparkMaster != null)
                {
                    break;
                }
            }

            if (sparkMaster == null)
            {
                _configuration = new DebugConfiguration(appConfig);
                _runMode = RunMode.DEBUG;
            }
            else if (sparkMaster.StartsWith("local"))
            {
                _configuration = new LocalConfiguration(appConfig);
                _runMode = RunMode.LOCAL;
            }
            else if (sparkMaster.StartsWith("spark://"))
            {
                _configuration = new DefaultConfiguration(appConfig);
                _runMode = RunMode.CLUSTER;
            }
            else if (sparkMaster.Equals("yarn-cluster", StringComparison.OrdinalIgnoreCase) ||
                     sparkMaster.Equals("yarn-client", StringComparison.OrdinalIgnoreCase) ||
                     sparkMaster.Equals("yarn", StringComparison.OrdinalIgnoreCase))
            {
                _configuration = new DefaultConfiguration(appConfig);
                _runMode = RunMode.YARN;
            }
            else
            {
                throw new NotSupportedException($"Unknown spark master value: {sparkMaster}");
            }

            _logger.LogInfo($"ConfigurationService runMode is {_runMode}");
        }

        /// <summary>
        /// Returns the port number for socket communication between JVM and CLR.
        /// </summary>
        public int BackendPortNumber => _configuration.GetPortNumber();

        /// <summary>
        /// Returns the worker executable path.
        /// </summary>
        /// <returns>Worker executable path</returns>
        public string GetWorkerExePath()
        {
            return _configuration.GetWorkerExePath();
        }

        /// <summary>
        /// Default configuration for Spark .NET jobs.
        /// Works with Standalone cluster mode.
        /// For Yarn, the worker should be available in the PATH environment
        /// variable for the user executing the job - normally the user is 'yarn'.
        /// </summary>
        private class DefaultConfiguration
        {
            protected readonly AppSettingsSection _appSettings;
            private readonly ILoggerService _logger =
                LoggerServiceFactory.GetLogger(typeof(DefaultConfiguration));
            private string _workerPath;

            internal DefaultConfiguration(Configuration configuration)
            {
                _appSettings = configuration.AppSettings;
            }

            /// <summary>
            /// The port number used for communicating with the .NET backend process.
            /// </summary>
            internal virtual int GetPortNumber()
            {
                if (!int.TryParse(
                    Environment.GetEnvironmentVariable(DotnetBackendPortEnvName),
                    out int portNumber))
                {
                    throw new Exception(
                        $"Environment variable {DotnetBackendPortEnvName} is not set.");
                }

                _logger.LogInfo($"Using port {portNumber} for connection.");

                return portNumber;
            }

            /// <summary>
            /// Returns the path of .NET worker process.
            /// </summary>
            /// <returns>The path of .NET worker process</returns>
            internal virtual string GetWorkerExePath()
            {
                if (_workerPath != null)
                {
                    return _workerPath;
                }

                KeyValueConfigurationElement workerPathConfig =
                    _appSettings.Settings[WorkerPathSettingKey];

                if (workerPathConfig == null)
                {
                    _workerPath = GetWorkerProcFileName();
                }
                else
                {
                    _workerPath = workerPathConfig.Value;
                    _logger.LogDebug($"Using .NET worker path from App.config: {_workerPath}");
                }

                return _workerPath;
            }

            internal virtual string GetWorkerProcFileName()
            {
                return s_procFileName;
            }
        }

        /// <summary>
        /// Configuration for local mode.
        /// </summary>
        private class LocalConfiguration : DefaultConfiguration
        {
            private readonly ILoggerService _logger =
                LoggerServiceFactory.GetLogger(typeof(LocalConfiguration));

            internal LocalConfiguration(Configuration configuration)
                : base(configuration)
            {
            }

            internal override string GetWorkerProcFileName()
            {
                string procFilePath;
                string procDir = Environment.GetEnvironmentVariable(WorkerPathSettingKey);
                if (!string.IsNullOrEmpty(procDir))
                {
                    procFilePath = Path.Combine(procDir, s_procFileName);
                    _logger.LogDebug($"Using the environment variable to construct .NET worker path: {procFilePath}.");
                    return procFilePath;
                }

                // Get the directory from the current assembly location.
                procDir = Path.GetDirectoryName(GetType().Assembly.Location);
                procFilePath = Path.Combine(procDir, s_procFileName);
                _logger.LogDebug($"Using the current assembly path to construct .NET worker path: {procFilePath}.");
                return procFilePath;
            }
        }

        /// <summary>
        /// Configuration for debug mode, which is only for the debugging/development purpose.
        /// </summary>
        private class DebugConfiguration : LocalConfiguration
        {
            private readonly ILoggerService _logger =
                LoggerServiceFactory.GetLogger(typeof(DebugConfiguration));

            internal DebugConfiguration(Configuration configuration)
                : base(configuration)
            {
            }

            internal override int GetPortNumber()
            {
                int portNumber = DotnetBackendDebugPort;

                KeyValueConfigurationElement portConfig =
                    _appSettings.Settings[DotnetBackendPortNumberSettingKey];

                if (portConfig != null)
                {
                    portNumber = int.Parse(portConfig.Value);
                }

                _logger.LogInfo($"Using port {portNumber} for connection.");

                return portNumber;
            }
        }
    }

    /// <summary>
    /// The running mode used by Configuration Service
    /// </summary>
    internal enum RunMode
    {
        /// <summary>
        /// Unknown running mode
        /// </summary>
        UNKNOWN,
        /// <summary>
        /// Debug mode, not a Spark mode but exists for develop debugging purpose
        /// </summary>
        DEBUG,
        /// <summary>
        /// Indicates service is running in local
        /// </summary>
        LOCAL,
        /// <summary>
        /// Indicates service is running in cluster
        /// </summary>
        CLUSTER,
        /// <summary>
        /// Indicates service is running in Yarn
        /// </summary>
        YARN
    }
}
