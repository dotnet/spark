// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Extensions.Hyperspace.Index
{
    /// <summary>
    /// Builder for <see cref="IndexConfig"/>.
    /// </summary>
    [HyperspaceSince(HyperspaceVersions.V0_0_1)]
    public sealed class Builder : IJvmObjectReferenceProvider
    {
        internal Builder(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Updates index name for <see cref="IndexConfig"/>.
        /// </summary>
        /// <param name="indexName">Index name for the <see cref="IndexConfig"/>.</param>
        /// <returns>An <see cref="Builder"/> object with updated indexname.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public Builder IndexName(string indexName)
        {
            Reference.Invoke("indexName", indexName);
            return this;
        }

        /// <summary>
        /// Updates column names for <see cref="IndexConfig"/>.
        ///
        /// Note: API signature supports passing one or more argument.
        /// </summary>
        /// <param name="indexedColumn">Indexed column for the
        /// <see cref="IndexConfig"/>.</param>
        /// <param name="indexedColumns">Indexed columns for the
        /// <see cref="IndexConfig"/>.</param>
        /// <returns>An <see cref="Builder"/> object with updated indexed columns.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public Builder IndexBy(string indexedColumn, params string[] indexedColumns)
        {
            Reference.Invoke("indexBy", indexedColumn, indexedColumns);
            return this;
        }

        /// <summary>
        /// Updates included columns for <see cref="IndexConfig"/>.
        /// 
        /// Note: API signature supports passing one or more argument.
        /// </summary>
        /// <param name="includedColumn">Included column for <see cref="IndexConfig"/>.
        /// </param>
        /// <param name="includedColumns">Included columns for <see cref="IndexConfig"/>.
        /// </param>
        /// <returns>An <see cref="Builder"/> object with updated included columns.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public Builder Include(string includedColumn, params string[] includedColumns)
        {
            Reference.Invoke("include", includedColumn, includedColumns);
            return this;
        }

        /// <summary>
        /// Creates IndexConfig from supplied index name, indexed columns and included columns
        /// to <see cref="Builder"/>.
        /// </summary>
        /// <returns>An <see cref="IndexConfig"/> object.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public IndexConfig Create() =>
            new IndexConfig((JvmObjectReference)Reference.Invoke("create"));
    }
}
