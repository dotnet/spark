// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// A set of methods for aggregations on a DataFrame.
    /// </summary>
    public sealed class RelationalGroupedDataset : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal RelationalGroupedDataset(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Compute aggregates by specifying a series of aggregate columns.
        /// </summary>
        /// <param name="expr">Column to aggregate on</param>
        /// <param name="exprs">Additional columns to aggregate on</param>
        /// <returns>New DataFrame object with aggregation applied</returns>
        public DataFrame Agg(Column expr, params Column[] exprs)
        {
            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("agg", expr, exprs));
        }

        /// <summary>
        /// Count the number of rows for each group.
        /// </summary>
        /// <returns>New DataFrame object with count applied</returns>
        public DataFrame Count()
        {
            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("count"));
        }
    }
}
