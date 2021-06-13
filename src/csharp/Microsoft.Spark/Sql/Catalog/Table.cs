// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Catalog
{
    /// <summary>
    /// A table in Spark, as returned by the `ListTables` method in `Catalog`.
    /// </summary>
    public sealed class Table : IJvmObjectReferenceProvider
    {
        internal Table(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Name of the database the table belongs to.
        /// </summary>
        /// <returns>string, the name of the database the table is in.</returns>
        public string Database => (string)Reference.Invoke("database");

        /// <summary>
        /// Description of the table.
        /// </summary>
        /// <returns>string, the description of the table.</returns>
        public string Description => (string)Reference.Invoke("description");

        /// <summary>
        /// Whether the table is temporary or not.
        /// </summary>
        /// <returns>bool, true if the table is temporary, false if it is not.</returns>
        public bool IsTemporary => (bool)Reference.Invoke("isTemporary");

        /// <summary>
        /// The name of the table.
        /// </summary>
        /// <returns>string, the name of the table.</returns>
        public string Name => (string)Reference.Invoke("name");

        /// <summary>
        /// The type of table (e.g. view/table)
        /// </summary>
        /// <returns>string, will return either `view` or `table` </returns>
        public string TableType => (string)Reference.Invoke("tableType");
    }
}
