// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Catalog
{
    /// <summary>
    /// A user-defined function in Spark, as returned by `ListFunctions` method in `Catalog`.
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
        /// Name of the database the function belongs to.
        /// </summary>
        /// <returns>string, the name of the database that the function is in.</returns>
        public string Database => (string)_jvmObject.Invoke("database");

        /// <summary>
        /// Description of the function.
        /// </summary>
        /// <returns>string, the description of the function.</returns>
        public string Description => (string)_jvmObject.Invoke("description");

        /// <summary>
        /// Whether the function is temporary or not
        /// </summary>
        /// <returns>bool, true if the function is temporary and false if it is not temporary.
        /// </returns>
        public bool IsTemporary => (bool)_jvmObject.Invoke("isTemporary");

        /// <summary>
        /// Name of the function
        /// </summary>
        /// <returns>string, the name of the function.</returns>
        public string Name => (string)_jvmObject.Invoke("name");

        /// <summary>
        /// The fully qualified class name of the function
        /// </summary>
        /// <returns>string, the name of the class that implements the function.</returns>
        public string ClassName => (string)_jvmObject.Invoke("className");
    }
}
