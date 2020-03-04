// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Dynamic;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Interop
{
    /// <summary>
    /// Contains everything needed to setup an environment for using .NET with Spark.
    /// </summary>
    internal static class SparkEnvironment
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(SparkEnvironment));

        private static Version GetSparkVersion()
        {
            var sparkVersion = new Version((string)JvmBridge.CallStaticJavaMethod(
                "org.apache.spark.deploy.dotnet.DotnetRunner",
                "SPARK_VERSION"));

            string sparkVersionOverride =
                Environment.GetEnvironmentVariable("SPARK_VERSION_OVERRIDE");
            if (!string.IsNullOrEmpty(sparkVersionOverride))
            {
                s_logger.LogInfo(
                    $"Overriding the Spark version from '{sparkVersion}' " +
                    $"to '{sparkVersionOverride}'.");
                sparkVersion = new Version(sparkVersionOverride);
            }

            return sparkVersion;
        }

        private static readonly Lazy<Version> s_sparkVersion =
            new Lazy<Version>(() => GetSparkVersion());
        internal static Version SparkVersion
        {
            get
            {
                return s_sparkVersion.Value;
            }
        }

        private static IJvmBridge s_jvmBridge;
        internal static IJvmBridge JvmBridge
        {
            get
            {
                if (s_jvmBridge == null)
                {
                    s_jvmBridge = new JvmBridge(ConfigurationService.GetBackendPortNumber());
                }

                return s_jvmBridge;
            }
            set
            {
                s_jvmBridge = value;
            }
        }

        private static IConfigurationService s_configurationService;
        internal static IConfigurationService ConfigurationService
        {
            get
            {
                return s_configurationService ??
                    (s_configurationService = new ConfigurationService());
            }
            set
            {
                s_configurationService = value;
            }
        }
    }
}
