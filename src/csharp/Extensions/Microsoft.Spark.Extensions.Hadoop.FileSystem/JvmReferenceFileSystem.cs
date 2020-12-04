// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Extensions.Hadoop.FileSystem
{
    /// <summary>
    /// <see cref="FileSystem"/> implementation that wraps a corresponding FileSystem object in the JVM.
    /// </summary>
    public class JvmReferenceFileSystem : FileSystem, IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal JvmReferenceFileSystem(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Delete a file.
        /// </summary>
        /// <param name="path">The path to delete.</param>
        /// <param name="recursive">If path is a directory and set to true, the directory is deleted else
        /// throws an exception. In case of a file the recursive can be set to either true or false.</param>
        /// <returns>True if delete is successful else false.</returns>
        public override bool Delete(string path, bool recursive = true)
        {
            JvmObjectReference pathObject =
                SparkEnvironment.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);

            return (bool)_jvmObject.Invoke("delete", pathObject, recursive);
        }

        public override void Dispose() => _jvmObject.Invoke("close");
    }
}
