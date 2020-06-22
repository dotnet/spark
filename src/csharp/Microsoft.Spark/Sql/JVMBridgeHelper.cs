// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using Microsoft.Spark.Services;
using Microsoft.Spark.Interop;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// An helper to launch dotnet jvm if needed
    /// </summary>    
    public class JVMBridgeHelper : IDisposable
    {
        /// <summary>
        /// Customization for JVM Bridge jar file
        /// If not exists, the helper will find out the jar in $DOTNET_WORKER_DIR folder.
        /// </summary>
        public static string JVMBridgeJarEnvName = "DOTNET_BRIDGE_JAR";

        /// <summary>
        /// DotnetRunner classname
        /// </summary>
        private static string RunnerClassname =
            "org.apache.spark.deploy.dotnet.DotnetRunner";

        private static string RunnerReadyMsg =
            ".NET Backend running debug mode. Press enter to exit";

        private static string RunnerAddressInUseMsg =
            "java.net.BindException: Address already in use";


        private static int maxWaitTimeoutMS = 60000;

        private readonly ILoggerService _logger =
            LoggerServiceFactory.GetLogger(typeof(JVMBridgeHelper));        

        /// <summary>
        /// The running jvm bridge process , null means no such process
        /// </summary>
        private Process jvmBridge;

        /// <summary>
        /// Detect if we already have the runner by checking backend port is using or not.
        /// </summary>
        /// <param name="customIPGlobalProperties">custom IPGlobalProperties, null for System.Net.NetworkInformation</param>
        /// <returns> True means backend port is occupied by the runner.</returns>
        public static bool IsDotnetBackendPortUsing(
            IPGlobalProperties customIPGlobalProperties = null)
        {
            var backendport = SparkEnvironment.ConfigurationService.GetBackendPortNumber();
            var listeningEndpoints =
                (customIPGlobalProperties ?? IPGlobalProperties.GetIPGlobalProperties())
                .GetActiveTcpListeners();
            return listeningEndpoints.Any(p => p.Port == backendport);
        }

        public JVMBridgeHelper()
        {
            var jarpath = locateBridgeJar();
            var sparksubmit = locateSparkSubmit();
            if (string.IsNullOrWhiteSpace(jarpath) ||
                string.IsNullOrWhiteSpace(sparksubmit))
            {
                // Cannot find correct launch informations, give up.
                return;
            }
            var arguments = $"--class {RunnerClassname} {jarpath} debug";
            var startupinfo = new ProcessStartInfo
            {
                FileName = sparksubmit,
                Arguments = arguments,
                RedirectStandardOutput = true,
                RedirectStandardInput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            jvmBridge = new Process() { StartInfo = startupinfo };
            _logger.LogInfo($"Launch JVM Bridge : {sparksubmit} {arguments}");
            jvmBridge.Start();

            // wait until we see .net backend started
            Task<string> message;
            while ((message = jvmBridge.StandardOutput.ReadLineAsync()) != null)
            {
                if (message.Wait(maxWaitTimeoutMS) == false)
                {
                    // wait timeout , giveup
                    break;
                }

                if (message.Result.Contains(RunnerReadyMsg))
                {
                    // launched successfully!
                    jvmBridge.StandardOutput.ReadToEndAsync();
                    _logger.LogInfo($"Launch JVM Bridge ready");
                    return;
                }
                if (message.Result.Contains(RunnerAddressInUseMsg))
                {
                    // failed to start for port is using, give up.
                    jvmBridge.StandardOutput.ReadToEndAsync();
                    break;
                }
            }
            // wait timeout , or failed to startup
            // give up.
            jvmBridge.Close();
            jvmBridge = null;
        }

        private string locateSparkSubmit()
        {
            var sparkHome = Environment.GetEnvironmentVariable("SPARK_HOME");
            if (string.IsNullOrWhiteSpace(sparkHome))
            {
                return string.Empty;
            }
            var filename = Path.Combine(sparkHome, "bin", "spark-submit");
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                filename += ".cmd";
            }
            if (!File.Exists(filename))
            {
                return string.Empty;
            }
            return filename;
        }

        private string locateBridgeJar()
        {
            var jarpath = Environment.GetEnvironmentVariable(JVMBridgeJarEnvName);
            if (string.IsNullOrWhiteSpace(jarpath) == false)
            {
                return jarpath;
            }

            var workdir = Environment.GetEnvironmentVariable(
                    ConfigurationService.WorkerDirEnvVarName);
            if ((workdir != null) && Directory.Exists(workdir))
            {
                // let's find the approicate jar in the work dirctory.
                var jarfile = new DirectoryInfo(workdir)
                    .GetFiles("microsoft-spark-*.jar")
                    .FirstOrDefault();
                if (jarfile != null)
                {
                    return Path.Combine(jarfile.DirectoryName, jarfile.Name);
                }
            }

            return string.Empty;
        }

        public void Dispose()
        {
            if (jvmBridge != null)
            {
                jvmBridge.StandardInput.WriteLine("\n");
                jvmBridge.WaitForExit(maxWaitTimeoutMS);
                _logger.LogInfo($"JVM Bridge disposed.");
            }
        }
    }
}