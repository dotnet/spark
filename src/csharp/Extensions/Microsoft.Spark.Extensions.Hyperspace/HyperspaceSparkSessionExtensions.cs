// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Extensions.Hyperspace
{
    /// <summary>
    /// Hyperspace-specific extension methods on <see cref="SparkSession"/>.
    /// </summary>
    [HyperspaceSince(HyperspaceVersions.V0_0_1)]
    public static class HyperspaceSparkSessionExtensions
    {
        private static readonly string s_pythonUtilsClassName =
            "com.microsoft.hyperspace.util.PythonUtils";

        /// <summary>
        /// Plug in Hyperspace-specific rules.
        /// </summary>
        /// <param name="session">A spark session that does not contain Hyperspace-specific rules.
        /// </param>
        /// <returns>A spark session that contains Hyperspace-specific rules.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public static SparkSession EnableHyperspace(this SparkSession session) =>
            new SparkSession(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_pythonUtilsClassName,
                    "enableHyperspace",
                    session));

        /// <summary>
        /// Plug out Hyperspace-specific rules.
        /// </summary>
        /// <param name="session">A spark session that contains Hyperspace-specific rules.</param>
        /// <returns>A spark session that does not contain Hyperspace-specific rules.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public static SparkSession DisableHyperspace(this SparkSession session) =>
            new SparkSession(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_pythonUtilsClassName,
                    "disableHyperspace",
                    session));

        /// <summary>
        /// Checks if Hyperspace is enabled or not.
        /// </summary>
        /// <param name="session"></param>
        /// <returns>True if Hyperspace is enabled or false otherwise.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public static bool IsHyperspaceEnabled(this SparkSession session) =>
            (bool)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_pythonUtilsClassName,
                    "isHyperspaceEnabled",
                    session);
    }
}
