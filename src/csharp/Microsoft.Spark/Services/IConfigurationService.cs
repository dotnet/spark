// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.Services
{
    /// <summary>
    /// Helps getting config settings to be used in .NET runtime
    /// </summary>
    internal interface IConfigurationService
    {
        /// <summary>
        /// The port number used for communicating with the .NET back-end process.
        /// </summary>
        int GetBackendPortNumber();

        /// <summary>
        /// The full path to the .NET worker executable.
        /// </summary>
        string GetWorkerExePath();
    }
}
