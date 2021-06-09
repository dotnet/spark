// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Extensions.Delta.Tables
{
    /// <summary>
    /// Builder class to specify the actions to perform when a source row has not matched any
    /// target Delta table row based on the merge condition, but has matched the additional
    /// condition if specified.
    /// 
    /// See <see cref="DeltaMergeBuilder"/> for more information.
    /// </summary>
    [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
    public class DeltaMergeNotMatchedActionBuilder : IJvmObjectReferenceProvider
    {
        internal DeltaMergeNotMatchedActionBuilder(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Insert a new row to the target table based on the rules defined by <c>values</c>.
        /// </summary>
        /// <param name="values">Rules to insert a row as a map between target column names and
        /// corresponding expressions as Column objects.</param>
        /// <returns>DeltaMergeBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeBuilder Insert(Dictionary<string, Column> values) =>
            new DeltaMergeBuilder((JvmObjectReference)Reference.Invoke("insert", values));

        /// <summary>
        /// Insert a new row to the target table based on the rules defined by <c>values</c>.
        /// </summary>
        /// <param name="values">Rules to insert a row as a map between target column names and
        /// corresponding expressions as SQL formatted strings.</param>
        /// <returns>DeltaMergeBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeBuilder InsertExpr(Dictionary<string, string> values) =>
            new DeltaMergeBuilder((JvmObjectReference)Reference.Invoke("insertExpr", values));

        /// <summary>
        /// Insert a new target Delta table row by assigning the target columns to the values of the
        /// corresponding columns in the source row.
        /// </summary>
        /// <returns>DeltaMergeBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeBuilder InsertAll() =>
            new DeltaMergeBuilder((JvmObjectReference)Reference.Invoke("insertAll"));
    }
}
