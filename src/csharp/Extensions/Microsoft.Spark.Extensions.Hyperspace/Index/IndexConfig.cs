// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Internal.Scala;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Extensions.Hyperspace.Index
{
    /// <summary>
    /// <see cref="IndexConfig"/> specifies the configuration of an index.
    /// </summary>
    [HyperspaceSince(HyperspaceVersions.V0_0_1)]
    public sealed class IndexConfig : IJvmObjectReferenceProvider
    {
        private static readonly string s_className = "com.microsoft.hyperspace.index.IndexConfig";

        /// <summary>
        /// <see cref="IndexConfig"/> specifies the configuration of an index.
        /// </summary>
        /// <param name="indexName">Index name.</param>
        /// <param name="indexedColumns">Columns from which an index is created.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public IndexConfig(string indexName, IEnumerable<string> indexedColumns)
            : this(indexName, indexedColumns, new string[] { })
        {
        }

        /// <summary>
        /// <see cref="IndexConfig"/> specifies the configuration of an index.
        /// </summary>
        /// <param name="indexName">Index name.</param>
        /// <param name="indexedColumns">Columns from which an index is created.</param>
        /// <param name="includedColumns">Columns to be included in the index.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public IndexConfig(
            string indexName,
            IEnumerable<string> indexedColumns,
            IEnumerable<string> includedColumns)
        {
            IndexName = indexName;
            IndexedColumns = new List<string>(indexedColumns);
            IncludedColumns = new List<string>(includedColumns);

            Reference = (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_className,
                "apply",
                IndexName,
                IndexedColumns,
                IncludedColumns);
        }

        /// <summary>
        /// <see cref="IndexConfig"/> specifies the configuration of an index.
        /// </summary>
        /// <param name="jvmObject">JVM object reference.</param>
        internal IndexConfig(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            IndexName = (string)Reference.Invoke("indexName");
            IndexedColumns = new List<string>(
                new Seq<string>((JvmObjectReference)Reference.Invoke("indexedColumns")));
            IncludedColumns = new List<string>(
                new Seq<string>((JvmObjectReference)Reference.Invoke("includedColumns")));
        }

        public JvmObjectReference Reference { get; private set; }

        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public string IndexName { get; private set; }

        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public List<string> IndexedColumns { get; private set; }

        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public List<string> IncludedColumns { get; private set; }

        /// <summary>
        /// Creates new <see cref="Builder"/> for constructing an
        /// <see cref="IndexConfig"/>.
        /// </summary>
        /// <returns>An <see cref="Builder"/> object.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public static Builder Builder() =>
            new Builder(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_className,
                    "builder"));

        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public override bool Equals(object that) => (bool)Reference.Invoke("equals", that);

        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public override int GetHashCode() => (int)Reference.Invoke("hashCode");

        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public override string ToString() => (string)Reference.Invoke("toString");
    }
}
