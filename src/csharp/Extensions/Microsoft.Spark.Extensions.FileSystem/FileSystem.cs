// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Extensions.FileSystem
{
    /// <summary>
    /// An abstract base class for a fairly generic filesystem. It may be implemented as a distributed
    /// filesystem, or as a "local" one that reflects the locally-connected disk. The local version exists
    /// for small Hadoop instances and for testing.
    /// 
    /// All user code that may potentially use the Hadoop Distributed File System should be written to use a FileSystem
    /// object. The Hadoop DFS is a multi-machine system that appears as a single disk. It's useful because of its fault
    /// tolerance and potentially very large capacity.
    /// </summary>
    public abstract class FileSystem : IDisposable
    {
        /// <summary>
        /// Returns the configured FileSystem implementation.
        /// </summary>
        /// <param name="sparkContext">The SparkContext whose configuration will be used.</param>
        /// <returns>The FileSystem.</returns>
        public static FileSystem Get(SparkContext sparkContext)
        {
            // TODO: Expose hadoopConfiguration as a .NET class and add an override for Get() that takes it.
            JvmObjectReference hadoopConfiguration = (JvmObjectReference)
                ((IJvmObjectReferenceProvider)sparkContext).Reference.Invoke("hadoopConfiguration");

            return new JvmReferenceFileSystem(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    "org.apache.hadoop.fs.FileSystem",
                    "get",
                    hadoopConfiguration));
        }

        /// <summary>
        /// Delete a file.
        /// </summary>
        /// <param name="path">The path to delete.</param>
        /// <param name="recursive">If path is a directory and set to true, the directory is deleted else
        /// throws an exception. In case of a file the recursive can be set to either true or false.</param>
        /// <returns>True if delete is successful else false.</returns>
        public abstract bool Delete(string path, bool recursive = true);

        public abstract void Dispose();
    }
}
