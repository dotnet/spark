// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Expressions;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// A set of methods for aggregations on a DataFrame.
    /// </summary>
    public sealed class RelationalGroupedDataset : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;
        private readonly DataFrame _dataFrame;

        internal RelationalGroupedDataset(JvmObjectReference jvmObject, DataFrame dataFrame)
        {
            _jvmObject = jvmObject;
            _dataFrame = dataFrame;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Compute aggregates by specifying a series of aggregate columns.
        /// </summary>
        /// <param name="expr">Column to aggregate on</param>
        /// <param name="exprs">Additional columns to aggregate on</param>
        /// <returns>New DataFrame object with aggregation applied</returns>
        public DataFrame Agg(Column expr, params Column[] exprs) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("agg", expr, exprs));

        /// <summary>
        /// Count the number of rows for each group.
        /// </summary>
        /// <returns>New DataFrame object with count applied</returns>
        public DataFrame Count() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("count"));

        /// <summary>
        /// Compute the mean value for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute mean on</param>
        /// <returns>New DataFrame object with mean applied</returns>
        public DataFrame Mean(params string[] colNames) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("mean", (object)colNames));

        /// <summary>
        /// Compute the max value for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute max on</param>
        /// <returns>New DataFrame object with max applied</returns>
        public DataFrame Max(params string[] colNames) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("max", (object)colNames));

        /// <summary>
        /// Compute the average value for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute average on</param>
        /// <returns>New DataFrame object with average applied</returns>
        public DataFrame Avg(params string[] colNames) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("avg", (object)colNames));

        /// <summary>
        /// Compute the min value for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute min on</param>
        /// <returns>New DataFrame object with min applied</returns>
        public DataFrame Min(params string[] colNames) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("min", (object)colNames));

        /// <summary>
        /// Compute the sum for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute sum on</param>
        /// <returns>New DataFrame object with sum applied</returns>
        public DataFrame Sum(params string[] colNames) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("sum", (object)colNames));

        internal DataFrame Apply(StructType returnType, Func<FxDataFrame, FxDataFrame> func)
        {
            DataFrameGroupedMapWorkerFunction.ExecuteDelegate wrapper =
                new DataFrameGroupedMapUdfWrapper(func).Execute;

            var udf = UserDefinedFunction.Create(
                _jvmObject.Jvm,
                func.Method.ToString(),
                CommandSerDe.Serialize(
                    wrapper,
                    CommandSerDe.SerializedMode.Row,
                    CommandSerDe.SerializedMode.Row),
                UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                returnType.Json);

            IReadOnlyList<string> columnNames = _dataFrame.Columns();
            var columns = new Column[columnNames.Count];
            for (int i = 0; i < columnNames.Count; ++i)
            {
                columns[i] = _dataFrame[columnNames[i]];
            }

            Column udfColumn = udf.Apply(columns);

            return new DataFrame((JvmObjectReference)_jvmObject.Invoke(
                "flatMapGroupsInPandas",
                udfColumn.Expr()));
        }
    }
}
