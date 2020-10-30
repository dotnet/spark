// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Services
{
    /// <summary>
    /// Helps getting config settings to be used in .NET runtime
    /// </summary>
    internal interface IConfigurationService
    {
        /// <summary>
        /// How often to run GC on JVM ThreadPool threads.
        /// </summary>
        TimeSpan JvmThreadGCInterval { get; }

        /// <summary>
        /// The port number used for communicating with the .NET backend process.
        /// </summary>
        int GetBackendPortNumber();

        /// <summary>
        /// Returns the max number of threads for socket communication between JVM and CLR.
        /// </summary>
        int GetNumBackendThreads();

        /// <summary>
        /// The full path to the .NET worker executable.
        /// </summary>
        string GetWorkerExePath();

        /// <summary>
        /// Flag indicating whether running in REPL.
        /// </summary>
        bool IsRunningRepl();
    }
}
