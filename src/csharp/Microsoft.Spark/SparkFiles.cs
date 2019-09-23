// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark
{
    /// <summary>
    /// Resolves paths to files added through `SparkContext.addFile()`.
    /// </summary>
    public static class SparkFiles
    {
        private static IJvmBridge Jvm { get; } = SparkEnvironment.JvmBridge;
        private static readonly string s_sparkFilesClassName = "org.apache.spark.SparkFiles";

        /// <summary>
        /// Get the absolute path of a file added through `SparkContext.addFile()`.
        /// </summary>
        /// <param name="fileName">The name of the file added through `SparkContext.addFile()`
        /// </param>
        /// <returns>The absolute path of the file.</returns>
        public static string Get(string fileName) =>
            (string)Jvm.CallStaticJavaMethod(s_sparkFilesClassName, "get", fileName);

        /// <summary>
        /// Get the root directory that contains files added through `SparkContext.addFile()`.
        /// </summary>
        /// <returns>The root directory that contains the files.</returns>
        public static string GetRootDirectory() =>
            (string)Jvm.CallStaticJavaMethod(s_sparkFilesClassName, "getRootDirectory");
    }
}
