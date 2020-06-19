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
    public sealed class IndexConfig : IJvmObjectReferenceProvider
    {
        private static readonly string s_className = "com.microsoft.hyperspace.index.IndexConfig";
        private readonly JvmObjectReference _jvmObject;

        /// <summary>
        /// <see cref="IndexConfig"/> specifies the configuration of an index.
        /// </summary>
        /// <param name="indexName">Index name.</param>
        /// <param name="indexedColumns">Columns from which an index is created.</param>
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
        public IndexConfig(
            string indexName,
            IEnumerable<string> indexedColumns,
            IEnumerable<string> includedColumns)
        {
            IndexName = indexName;
            IndexedColumns = new List<string>(indexedColumns);
            IncludedColumns = new List<string>(includedColumns);

            _jvmObject = (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
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
            _jvmObject = jvmObject;
            IndexName = (string)_jvmObject.Invoke("indexName");
            IndexedColumns = new List<string>((string[])_jvmObject.Invoke("indexedColumns"));
            IncludedColumns = new List<string>((string[])_jvmObject.Invoke("includedColumns"));
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        public string IndexName { get; private set; }

        public List<string> IndexedColumns { get; private set; }

        public List<string> IncludedColumns { get; private set; }

        /// <summary>
        /// Creates new <see cref="Builder"/> for constructing an
        /// <see cref="IndexConfig"/>.
        /// </summary>
        /// <returns>An <see cref="Builder"/> object.</returns>
        public static Builder Builder() =>
            new Builder(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_className,
                    "builder"));

        public override bool Equals(object that) => (bool)_jvmObject.Invoke("equals", that);

        public override int GetHashCode() => (int)_jvmObject.Invoke("hashCode");

        public override string ToString() => (string)_jvmObject.Invoke("toString");
    }
}
