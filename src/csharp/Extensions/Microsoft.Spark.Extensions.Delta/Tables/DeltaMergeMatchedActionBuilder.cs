// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Extensions.Delta.Tables
{
    /// <summary>
    /// Builder class to specify the actions to perform when a target table row has matched a
    /// source row based on the given merge condition and optional match condition.
    /// </summary>
    [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
    public class DeltaMergeMatchedActionBuilder : IJvmObjectReferenceProvider
    {
        internal DeltaMergeMatchedActionBuilder(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Update the matched table rows based on the rules defined by <c>set</c>.
        /// </summary>
        /// <param name="set">Rules to update a row as amap between target column names and
        /// corresponding update expressions as Column objects.</param>
        /// <returns>DeltaMergeBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeBuilder Update(Dictionary<string, Column> set) =>
            new DeltaMergeBuilder((JvmObjectReference)Reference.Invoke("update", set));

        /// <summary>
        /// Update the matched table rows based on the rules defined by <c>set</c>.
        /// </summary>
        /// <param name="set">Rules to update a row as a map between target column names and
        /// corresponding update expressions as SQL formatted strings.</param>
        /// <returns>DeltaMergeBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeBuilder UpdateExpr(Dictionary<string, string> set) =>
            new DeltaMergeBuilder((JvmObjectReference)Reference.Invoke("updateExpr", set));

        /// <summary>
        /// Update all the columns of the matched table row with the values of the corresponding
        /// columns in the source row.
        /// </summary>
        /// <returns>DeltaMergeBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeBuilder UpdateAll() =>
            new DeltaMergeBuilder((JvmObjectReference)Reference.Invoke("updateAll"));

        /// <summary>
        /// Delete a matched row from the table.
        /// </summary>
        /// <returns>DeltaMergeBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeBuilder Delete() =>
            new DeltaMergeBuilder((JvmObjectReference)Reference.Invoke("delete"));
    }
}
