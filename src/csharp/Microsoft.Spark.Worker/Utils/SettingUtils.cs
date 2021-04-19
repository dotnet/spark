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
        internal static string GetWorkerFactorySecret() =>
            GetEnvironmentVariable("PYTHON_WORKER_FACTORY_SECRET");

        internal static int GetWorkerFactoryPort() =>
            int.Parse(GetEnvironmentVariable("PYTHON_WORKER_FACTORY_PORT").Trim());
    }
}
