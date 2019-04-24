// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Runtime.CompilerServices;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Services;

// TODO: Move the followings to the AssemblyInfo.cs.
[assembly: InternalsVisibleTo("Microsoft.Spark.UnitTest")]
[assembly: InternalsVisibleTo("Microsoft.Spark.Worker")]
[assembly: InternalsVisibleTo("Microsoft.Spark.Worker.UnitTest")]
[assembly: InternalsVisibleTo("Microsoft.Spark.E2ETest")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")] // For Moq

namespace Microsoft.Spark.Interop
{
    /// <summary>
    /// Contains everything needed to setup an environment for using .NET with Spark.
    /// </summary>
    internal static class SparkEnvironment
    {
        private static IJvmBridge s_jvmBridge;
        internal static IJvmBridge JvmBridge
        {
            get
            {
                if (s_jvmBridge == null)
                {
                    s_jvmBridge = new JvmBridge(ConfigurationService.BackendPortNumber);
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
