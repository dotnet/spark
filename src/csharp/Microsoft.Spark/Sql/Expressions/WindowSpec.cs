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
        internal WindowSpec(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Defines the partitioning columns in a `WindowSpec`.
        /// </summary>
        /// <param name="colName">Name of column</param>
        /// <param name="colNames">Additional column names</param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec PartitionBy(string colName, params string[] colNames) =>
            WrapAsWindowSpec(Reference.Invoke("partitionBy", colName, colNames));

        /// <summary>
        /// Defines the partitioning columns in a `WindowSpec`.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec PartitionBy(params Column[] columns) =>
            WrapAsWindowSpec(Reference.Invoke("partitionBy", (object)columns));

        /// <summary>
        /// Defines the ordering columns in a `WindowSpec`.
        /// </summary>
        /// <param name="colName">Name of column</param>
        /// <param name="colNames">Additional column names</param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec OrderBy(string colName, params string[] colNames) =>
            WrapAsWindowSpec(Reference.Invoke("orderBy", colName, colNames));

        /// <summary>
        /// Defines the ordering columns in a `WindowSpec`.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>WindowSpec object</returns>
        public WindowSpec OrderBy(params Column[] columns) =>
            WrapAsWindowSpec(Reference.Invoke("orderBy", (object)columns));

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
            WrapAsWindowSpec(Reference.Invoke("rowsBetween", start, end));

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
            WrapAsWindowSpec(Reference.Invoke("rangeBetween", start, end));

        /// <summary>
        /// Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
        /// </summary>
        /// <remarks>
        /// This API is deprecated in Spark 2.4 and removed in Spark 3.0.
        /// </remarks>
        /// <param name="start">
        /// Boundary start, inclusive. The frame is unbounded if the expression is
        /// `Microsoft.Spark.Sql.Functions.UnboundedPreceding()`
        /// </param>
        /// <param name="end">
        /// Boundary end, inclusive. The frame is unbounded if the expression is
        /// `Microsoft.Spark.Sql.Functions.UnboundedFollowing()`
        /// </param>
        /// <returns>WindowSpec object</returns>
        [Deprecated(Versions.V2_4_0)]
        [Removed(Versions.V3_0_0)]
        public WindowSpec RangeBetween(Column start, Column end) =>
            WrapAsWindowSpec(Reference.Invoke("rangeBetween", start, end));

        private WindowSpec WrapAsWindowSpec(object obj) => new WindowSpec((JvmObjectReference)obj);
    }
}
