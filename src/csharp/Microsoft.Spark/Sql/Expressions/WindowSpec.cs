// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Expressions
{
    /// <summary>
    /// A window specification that defines the partitioning, ordering, and frame boundaries.
    /// </summary>
    public sealed class WindowSpec : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal WindowSpec(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Defines the partitioning columns in a `WindowSpec`.
        /// </summary>
        /// <param name="colName">Name of column</param>
        /// <param name="colNames">Additional column names</param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec PartitionBy(string colName, params string[] colNames) =>
            WrapAsWindowSpec(_jvmObject.Invoke("partitionBy", colName, colNames));

        /// <summary>
        /// Defines the partitioning columns in a `WindowSpec`.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec PartitionBy(params Column[] columns) =>
            WrapAsWindowSpec(_jvmObject.Invoke("partitionBy", (object)columns));

        /// <summary>
        /// Defines the ordering columns in a `WindowSpec`.
        /// </summary>
        /// <param name="colName">Name of column</param>
        /// <param name="colNames">Additional column names</param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec OrderBy(string colName, params string[] colNames) =>
            WrapAsWindowSpec(_jvmObject.Invoke("orderBy", colName, colNames));

        /// <summary>
        /// Defines the ordering columns in a `WindowSpec`.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec OrderBy(params Column[] columns) =>
            WrapAsWindowSpec(_jvmObject.Invoke("orderBy", (object)columns));

        /// <summary>
        /// Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
        /// </summary>
        /// <param name="start">
        /// Boundary start, inclusive. The frame is unbounded if this is
        /// the minimum long value `Window.s_unboundedPreceding`.
        /// </param>
        /// <param name="end">
        /// Boundary end, inclusive. The frame is unbounded if this is the
        /// maximum long value `Window.s_unboundedFollowing`.
        /// </param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec RowsBetween(long start, long end) =>
            WrapAsWindowSpec(_jvmObject.Invoke("rowsBetween", start, end));

        /// <summary>
        /// Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
        /// </summary>
        /// <param name="start">
        /// Boundary start, inclusive. The frame is unbounded if this is
        /// the minimum long value `Window.s_unboundedPreceding`.
        /// </param>
        /// <param name="end">
        /// Boundary end, inclusive. The frame is unbounded if this is
        /// maximum long value `Window.s_unboundedFollowing`.
        /// </param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec RangeBetween(long start, long end) =>
            WrapAsWindowSpec(_jvmObject.Invoke("rangeBetween", start, end));

        /// <summary>
        /// Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
        /// </summary>
        /// <param name="start">
        /// Boundary start, inclusive. The frame is unbounded if the expression is
        /// `Microsoft.Spark.Sql.Functions.UnboundedPreceding()`
        /// </param>
        /// <param name="end">
        /// Boundary end, inclusive. The frame is unbounded if the expression is
        /// `Microsoft.Spark.Sql.Functions.UnboundedFollowing()`
        /// </param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec RangeBetween(Column start, Column end) =>
            WrapAsWindowSpec(_jvmObject.Invoke("rangeBetween", start, end));

        private WindowSpec WrapAsWindowSpec(object obj) => new WindowSpec((JvmObjectReference)obj);
    }
}
