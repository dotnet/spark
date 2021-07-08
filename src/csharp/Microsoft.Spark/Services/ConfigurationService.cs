// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Runtime.InteropServices;
using static System.Environment;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Services
{
    /// <summary>
    /// Implementation of configuration service that helps getting config settings
    /// to be used in .NET backend.
    /// </summary>
    internal sealed class ConfigurationService : IConfigurationService
    {
        public const string WorkerReadBufferSizeEnvVarName = "spark.dotnet.worker.readBufferSize";
        public const string WorkerWriteBufferSizeEnvVarName =
            "spark.dotnet.worker.writeBufferSize";

        internal const string DefaultWorkerDirEnvVarName = "DOTNET_WORKER_DIR";
        internal const string WorkerVerDirEnvVarNameFormat = "DOTNET_WORKER_{0}_DIR";

        private const string DotnetBackendPortEnvVarName = "DOTNETBACKEND_PORT";
        private const int DotnetBackendDebugPort = 5567;

        private const string DotnetNumBackendThreadsEnvVarName = "DOTNET_SPARK_NUM_BACKEND_THREADS";
        private const int DotnetNumBackendThreadsDefault = 10;

        private static readonly string s_procBaseFileName = "Microsoft.Spark.Worker";

        private readonly ILoggerService _logger =
            LoggerServiceFactory.GetLogger(typeof(ConfigurationService));

        private string _workerPath;
        private string _workerDirEnvVarName;

        /// <summary>
        /// The Microsoft.Spark.Worker filename.
        /// </summary>
        internal static string ProcFileName { get; } = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ?
            $"{s_procBaseFileName}.exe" :
            s_procBaseFileName;

        /// <summary>
        /// Returns the environment variable name that defines the path to the
        /// Microsoft.Spark.Worker. Using the Microsoft.Spark assembly version
        /// we use the first environment variable that is defined in the
        /// following order and default to DOTNET_WORKER_DIR if not:
        /// 
        /// - DOTNET_WORKER_{MAJOR}_{MINOR}_{BUILD}_DIR
        /// - DOTNET_WORKER_{MAJOR}_{MINOR}_DIR
        /// - DOTNET_WORKER_{MAJOR}_DIR
        /// </summary>
        internal string WorkerDirEnvVarName
        {
            get
            {
                if (_workerDirEnvVarName != null)
                {
                    return _workerDirEnvVarName;
                }

                var version = new Version(AssemblyInfoProvider.MicrosoftSparkAssemblyInfo().AssemblyVersion);
                var versionComponents = new int[] { version.Major, version.Minor, version.Build };
                for (int i = versionComponents.Length; i > 0; --i)
                {
                    var span = new ReadOnlySpan<int>(versionComponents, 0, i);
                    string verEnvVarName = string.Format(
                        WorkerVerDirEnvVarNameFormat,
                        string.Join("_", span.ToArray()));
                    if (!string.IsNullOrWhiteSpace(GetEnvironmentVariable(verEnvVarName)))
                    {
                        _workerDirEnvVarName = verEnvVarName;
                        return _workerDirEnvVarName;
                    }
                }

                _workerDirEnvVarName = DefaultWorkerDirEnvVarName;
                return _workerDirEnvVarName;
            }
        }

        /// <summary>
        /// How often to run GC on JVM ThreadPool threads. Defaults to 5 minutes.
        /// </summary>
        public TimeSpan JvmThreadGCInterval
        {
            get
            {
                string envVar = GetEnvironmentVariable("DOTNET_JVM_THREAD_GC_INTERVAL");
                return string.IsNullOrEmpty(envVar) ? TimeSpan.FromMinutes(5) : TimeSpan.Parse(envVar);
            }
        }

        internal static bool IsDatabricks { get; } =
            !string.IsNullOrEmpty(GetEnvironmentVariable("DATABRICKS_RUNTIME_VERSION"));

        /// <summary>
        /// Returns the port number for socket communication between JVM and CLR.
        /// </summary>
        public int GetBackendPortNumber()
        {
            if (!int.TryParse(
                GetEnvironmentVariable(DotnetBackendPortEnvVarName),
                out int portNumber))
            {
                _logger.LogInfo($"'{DotnetBackendPortEnvVarName}' environment variable is not set.");
                portNumber = DotnetBackendDebugPort;
            }

            _logger.LogInfo($"Using port {portNumber} for connection.");

            return portNumber;
        }

        /// <summary>
        /// Returns the max number of threads for socket communication between JVM and CLR.
        /// </summary>
        public int GetNumBackendThreads()
        {
            if (!int.TryParse(
                GetEnvironmentVariable(DotnetNumBackendThreadsEnvVarName),
                out int numThreads))
            {
                numThreads = DotnetNumBackendThreadsDefault;
            }

            return numThreads;
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

            // If the WorkerDirEnvName environment variable is set, the worker path is constructed
            // based on it.
            string workerDir = GetEnvironmentVariable(WorkerDirEnvVarName);
            if (!string.IsNullOrEmpty(workerDir))
            {
                _workerPath = Path.Combine(workerDir, ProcFileName);
                _logger.LogDebug(
                    "Using the {0} environment variable to construct .NET worker path: {1}.",
                    WorkerDirEnvVarName,
                    _workerPath);
                return _workerPath;
            }

            // Otherwise, the worker executable name is returned meaning it should be PATH.
            _workerPath = ProcFileName;
            return _workerPath;
        }

        /// <summary>
        /// Flag indicating whether running in REPL.
        /// </summary>
        public bool IsRunningRepl() =>
            EnvironmentUtils.GetEnvironmentVariableAsBool(Constants.RunningREPLEnvVar);
    }
}
