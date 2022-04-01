// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Interop
{
    /// <summary>
    /// Contains everything needed to setup an environment for using .NET with Spark.
    /// </summary>
    public static class SparkEnvironment
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

        private static IJvmBridgeFactory s_jvmBridgeFactory;
        internal static IJvmBridgeFactory JvmBridgeFactory
        {
            get
            {
                return s_jvmBridgeFactory ??= new JvmBridgeFactory();
            }
            set
            {
                s_jvmBridgeFactory = value;
            }
        }

        private static IJvmBridge s_jvmBridge;
        /// <summary>
        /// The bridge between the JVM and the CLR.
        /// </summary>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        public static IJvmBridge JvmBridge
        {
            get
            {
                return s_jvmBridge ??=
                    JvmBridgeFactory.Create(ConfigurationService.GetBackendPortNumber());
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
                return s_configurationService ??= new ConfigurationService();
            }
            set
            {
                s_configurationService = value;
            }
        }

        private static CallbackServer s_callbackServer;
        internal static CallbackServer CallbackServer
        {
            get
            {
                return s_callbackServer ??= new CallbackServer(JvmBridge);
            }
        }
    }
}
