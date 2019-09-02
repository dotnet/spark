// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Catalog
{
    /// <summary>
    /// A user-defined function in Spark Catalog.
    /// </summary>
    public sealed class Function : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal Function(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Name of the database the function belongs to
        /// </summary>
        /// <returns>string</returns>
        public string Database => (string)_jvmObject.Invoke("database");

        /// <summary>
        /// Description of the function
        /// </summary>
        /// <returns>string</returns>
        public string Description => (string)_jvmObject.Invoke("description");

        /// <summary>
        /// Whether the function is temporary or not
        /// </summary>
        /// <returns>string</returns>
        public bool IsTemporary => (bool)_jvmObject.Invoke("isTemporary");

        /// <summary>
        /// Name of the function
        /// </summary>
        /// <returns>string</returns>
        public string Name => (string)_jvmObject.Invoke("name");

        /// <summary>
        /// The fully qualified class name of the function
        /// </summary>
        /// <returns>string</returns>
        public string ClassName => (string)_jvmObject.Invoke("className");
    }
}
