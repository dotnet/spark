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
    /// Resolves paths to files added through <see cref="SparkContext.AddFile(string, bool)"/>.
    /// </summary>
    public static class SparkFiles
    {
        private static IJvmBridge Jvm { get; } = SparkEnvironment.JvmBridge;
        private static readonly string s_sparkFilesClassName = "org.apache.spark.SparkFiles";

        [ThreadStatic]
        private static string s_rootDirectory;

        [ThreadStatic]
        private static bool s_isRunningOnWorker;

        /// <summary>
        /// Get the absolute path of a file added through
        /// <see cref="SparkContext.AddFile(string, bool)"/>.
        /// </summary>
        /// <param name="fileName">The name of the file added through
        /// <see cref="SparkContext.AddFile(string, bool)"/>.
        /// </param>
        /// <returns>The absolute path of the file.</returns>
        public static string Get(string fileName) =>
            Path.GetFullPath(Path.Combine(GetRootDirectory(), fileName));

        /// <summary>
        /// Get the root directory that contains files added through
        /// <see cref="SparkContext.AddFile(string, bool)"/>.
        /// </summary>
        /// <returns>The root directory that contains the files.</returns>
        public static string GetRootDirectory() =>
            s_isRunningOnWorker ?
            s_rootDirectory :
            (string)Jvm.CallStaticJavaMethod(s_sparkFilesClassName, "getRootDirectory");

        /// <summary>
        /// Set the root directory that contains files added through
        /// <see cref="SparkContext.AddFile(string, bool)"/>.
        /// <remarks>
        /// This should only be called from the Microsoft.Spark.Worker.
        /// </remarks>
        /// </summary>
        /// <param name="path">Root directory that contains files added
        /// through <see cref="SparkContext.AddFile(string, bool)"/>.
        /// </param>
        internal static void SetRootDirectory(string path)
        {
            s_isRunningOnWorker = true;
            s_rootDirectory = path;
        }
    }
}
