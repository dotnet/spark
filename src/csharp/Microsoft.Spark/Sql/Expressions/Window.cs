// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Expressions
{
    /// <summary>
    /// Utility functions for defining window in DataFrames.
    /// </summary>
    public static class Window
    {
        private static IJvmBridge Jvm { get; } = SparkEnvironment.JvmBridge;
        private static readonly string s_windowClassName =
            "org.apache.spark.sql.expressions.Window";

        /// <summary>
        /// Value representing the first row in the partition, equivalent to
        /// "UNBOUNDED PRECEDING" in SQL.
        /// </summary>
        public static long UnboundedPreceding =>
            (long)Jvm.CallStaticJavaMethod(s_windowClassName, "unboundedPreceding");

        /// <summary>
        /// Value representing the last row in the partition, equivalent to
        /// "UNBOUNDED FOLLOWING" in SQL.
        /// </summary>
        public static long UnboundedFollowing =>
            (long)Jvm.CallStaticJavaMethod(s_windowClassName, "unboundedFollowing");

        /// <summary>
        /// Value representing the current row.
        /// </summary>
        public static long CurrentRow =>
            (long)Jvm.CallStaticJavaMethod(s_windowClassName, "currentRow");

        /// <summary>
        /// Creates a `WindowSpec` with the partitioning defined.
        /// </summary>
        /// <param name="colName">Name of column</param>
        /// <param name="colNames">Additional column names</param>
        /// <returns>WindowSpec object</returns>
        public static WindowSpec PartitionBy(string colName, params string[] colNames) =>
            Apply("partitionBy", colName, colNames);

        /// <summary>
        /// Creates a `WindowSpec` with the partitioning defined.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>WindowSpec object</returns>
        public static WindowSpec PartitionBy(params Column[] columns) =>
            Apply("partitionBy", (object)columns);

        /// <summary>
        /// Creates a `WindowSpec` with the ordering defined.
        /// </summary>
        /// <param name="colName">Name of column</param>
        /// <param name="colNames">Additional column names</param>
        /// <returns>WindowSpec object</returns>
        public static WindowSpec OrderBy(string colName, params string[] colNames) =>
            Apply("orderBy", colName, colNames);

        /// <summary>
        /// Creates a `WindowSpec` with the ordering defined.
        /// </summary>
        /// <param name="columns">Column expressions</param>
        /// <returns>WindowSpec object</returns>
        public static WindowSpec OrderBy(params Column[] columns) =>
            Apply("orderBy", (object)columns);

        /// <summary>
        /// Creates a `WindowSpec` with the frame boundaries defined,
        /// from `start` (inclusive) to `end` (inclusive).
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
        public static WindowSpec RowsBetween(long start, long end) =>
            Apply("rowsBetween", start, end);

        /// <summary>
        /// Creates a `WindowSpec` with the frame boundaries defined,
        /// from `start` (inclusive) to `end` (inclusive).
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
        public static WindowSpec RangeBetween(long start, long end) =>
            Apply("rangeBetween", start, end);

        /// <summary>
        /// Creates a `WindowSpec` with the frame boundaries defined,
        /// from `start` (inclusive) to `end` (inclusive).
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
        public static WindowSpec RangeBetween(Column start, Column end) =>
            Apply("rangeBetween", start, end);

        private static WindowSpec Apply(string methodName, object arg)
        {
            return new WindowSpec(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_windowClassName,
                    methodName,
                    arg));
        }

        private static WindowSpec Apply(string methodName, object arg1, object arg2)
        {
            return new WindowSpec(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_windowClassName,
                    methodName,
                    arg1,
                    arg2));
        }
    }
}
