// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using static System.Environment;

namespace Microsoft.Spark.Worker.Utils
{
    /// <summary>
    /// Provides functionalities to retrieve various settings.
    /// </summary>
    internal static class SettingUtils
    {
        internal static bool IsDatabricks { get; } =
            !string.IsNullOrEmpty(GetEnvironmentVariable("DATABRICKS_RUNTIME_VERSION"));

        internal static string GetWorkerFactorySecret(Version version)
        {
            return (version >= new Version(Versions.V2_3_1)) ?
                GetEnvironmentVariable("PYTHON_WORKER_FACTORY_SECRET") :
                null;
        }

        internal static int GetWorkerFactoryPort(Version version)
        {
            string portStr = (version >= new Version(Versions.V2_3_1)) ?
                GetEnvironmentVariable("PYTHON_WORKER_FACTORY_PORT") :
                Console.ReadLine();

            return int.Parse(portStr.Trim());
        }
    }
}
