// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Catalog
{
    /// <summary>
    /// A database in Spark, as returned by the `ListDatabases` method defined in `Catalog`.
    /// </summary>
    public sealed class Database : IJvmObjectReferenceProvider
    {
        internal Database(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Description of the database.
        /// </summary>
        /// <returns>string, the description of the database.</returns>
        public string Description => (string)Reference.Invoke("description");

        /// <summary>
        /// Path (in the form of a uri) to data files
        /// </summary>
        /// <returns>string, the location of the database.</returns>
        public string LocationUri => (string)Reference.Invoke("locationUri");

        /// <summary>
        /// Name of the database.
        /// </summary>
        /// <returns>string, the name of the database.</returns>
        public string Name => (string)Reference.Invoke("name");
    }
}
