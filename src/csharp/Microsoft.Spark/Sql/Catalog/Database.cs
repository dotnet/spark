// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Catalog
{
    /// <summary>
    /// A database in Spark, defined in Catalog.
    /// </summary>
    public sealed class Database : IJvmObjectReferenceProvider
    {
        private JvmObjectReference _jvmObject;

        internal Database(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
        
        /// <summary>
        /// Description of the database.
        /// </summary>
        /// <returns>string</returns>
        public string Description => (string)_jvmObject.Invoke("description");

        /// <summary>
        /// Path (in the form of a uri) to data files
        /// </summary>
        /// <returns>string</returns>
        public string LocationUri => (string)_jvmObject.Invoke("locationUri");

        /// <summary>
        /// Name of the database.
        /// </summary>
        /// <returns>string</returns>
        public string Name => (string)_jvmObject.Invoke("name");
    }
}
