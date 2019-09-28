// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark
{
    /// <summary>
    /// Resolves paths to files added through
    /// <c>SparkContext.addFile()</c>.
    /// </summary>
    public static class SparkFiles
    {
        private static IJvmBridge Jvm { get; } = SparkEnvironment.JvmBridge;
        private static readonly string s_sparkFilesClassName = "org.apache.spark.SparkFiles";

        [ThreadStatic]
        private static string s_rootDirectory;

        [ThreadStatic]
        private static bool s_isWorker;

        /// <summary>
        /// Get the absolute path of a file added through
        /// <c>SparkContext.addFile()</c>.
        /// </summary>
        /// <param name="fileName">The name of the file added
        /// through <c>SparkContext.addFile()</c>
        /// </param>
        /// <returns>The absolute path of the file.</returns>
        public static string Get(string fileName) => Path.Combine(GetRootDirectory(), fileName);

        /// <summary>
        /// Get the root directory that contains files added
        /// through <c>SparkContext.addFile()</c>.
        /// </summary>
        /// <returns>The root directory that contains the files.</returns>
        public static string GetRootDirectory() =>
            s_isWorker ?
            s_rootDirectory :
            (string)Jvm.CallStaticJavaMethod(s_sparkFilesClassName, "getRootDirectory");

        /// <summary>
        /// Set the root directory that contains files added
        /// through <c>SparkContext.addFile()</c>
        /// <remarks>
        /// This should only be called from the Microsoft.Spark.Worker.
        /// </remarks>
        /// </summary>
        /// <param name="path">Root directory that contains files added
        /// through <c>SparkContext.addFile()</c>.
        /// </param>
        internal static void SetRootDirectory(string path)
        {
            s_isWorker = true;
            s_rootDirectory = path;
        }
    }
}
