// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Hadoop.Conf;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Hadoop.Fs
{
    /// <summary>
    /// A fairly generic filesystem. It may be implemented as a distributed filesystem, or as a "local" one
    /// that reflects the locally-connected disk. The local version exists for small Hadoop instances and for
    /// testing.
    /// 
    /// All user code that may potentially use the Hadoop Distributed File System should be written to use a
    /// FileSystem object. The Hadoop DFS is a multi-machine system that appears as a single disk. It's
    /// useful because of its fault tolerance and potentially very large capacity.
    /// </summary>
    public class FileSystem : IJvmObjectReferenceProvider, IDisposable
    {
        internal FileSystem(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Returns the configured <see cref="FileSystem"/>.
        /// </summary>
        /// <param name="conf">The configuration to use.</param>
        /// <returns>The FileSystem.</returns>
        public static FileSystem Get(Configuration conf) =>
            new FileSystem((JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "org.apache.hadoop.fs.FileSystem",
                "get",
                conf));

        /// <summary>
        /// Delete a file.
        /// </summary>
        /// <param name="path">The path to delete.</param>
        /// <param name="recursive">If path is a directory and set to true, the directory is deleted else
        /// throws an exception. In case of a file the recursive can be set to either true or false.</param>
        /// <returns>True if delete is successful else false.</returns>
        public bool Delete(string path, bool recursive = true)
        {
            JvmObjectReference pathObject =
                SparkEnvironment.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);

            return (bool)Reference.Invoke("delete", pathObject, recursive);
        }

        /// <summary>
        /// Check if a path exists.
        /// </summary>
        /// <param name="path">Source path</param>
        /// <returns>True if the path exists else false.</returns>
        public bool Exists(string path)
        {
            JvmObjectReference pathObject =
                SparkEnvironment.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);

            return (bool)Reference.Invoke("exists", pathObject);
        }

        public void Dispose() => Reference.Invoke("close");
    }
}
