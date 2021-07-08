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
        private readonly DataFrame _dataFrame;

        internal RelationalGroupedDataset(JvmObjectReference jvmObject, DataFrame dataFrame)
        {
            Reference = jvmObject;
            _dataFrame = dataFrame;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Compute aggregates by specifying a series of aggregate columns.
        /// </summary>
        /// <param name="expr">Column to aggregate on</param>
        /// <param name="exprs">Additional columns to aggregate on</param>
        /// <returns>New DataFrame object with aggregation applied</returns>
        public DataFrame Agg(Column expr, params Column[] exprs) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("agg", expr, exprs));

        /// <summary>
        /// Count the number of rows for each group.
        /// </summary>
        /// <returns>New DataFrame object with count applied</returns>
        public DataFrame Count() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("count"));

        /// <summary>
        /// Compute the mean value for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute mean on</param>
        /// <returns>New DataFrame object with mean applied</returns>
        public DataFrame Mean(params string[] colNames) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("mean", (object)colNames));

        /// <summary>
        /// Compute the max value for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute max on</param>
        /// <returns>New DataFrame object with max applied</returns>
        public DataFrame Max(params string[] colNames) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("max", (object)colNames));

        /// <summary>
        /// Compute the average value for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute average on</param>
        /// <returns>New DataFrame object with average applied</returns>
        public DataFrame Avg(params string[] colNames) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("avg", (object)colNames));

        /// <summary>
        /// Compute the min value for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute min on</param>
        /// <returns>New DataFrame object with min applied</returns>
        public DataFrame Min(params string[] colNames) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("min", (object)colNames));

        /// <summary>
        /// Compute the sum for each numeric columns for each group.
        /// </summary>
        /// <param name="colNames">Name of columns to compute sum on</param>
        /// <returns>New DataFrame object with sum applied</returns>
        public DataFrame Sum(params string[] colNames) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("sum", (object)colNames));

        /// <summary>
        /// Pivots a column of the current DataFrame and performs the specified aggregation.
        /// </summary>
        /// <param name="pivotColumn">Name of the column to pivot</param>
        /// <returns>New RelationalGroupedDataset object with pivot applied</returns>
        public RelationalGroupedDataset Pivot(string pivotColumn) => 
            new RelationalGroupedDataset(
                (JvmObjectReference)Reference.Invoke("pivot", pivotColumn), _dataFrame);

        /// <summary>
        /// Pivots a column of the current DataFrame and performs the specified aggregation.
        /// </summary>
        /// <param name="pivotColumn">Name of the column to pivot of type string</param>
        /// <param name="values">List of values that will be translated to columns in the
        /// output DataFrame.</param>
        /// <returns>New RelationalGroupedDataset object with pivot applied</returns>
        public RelationalGroupedDataset Pivot(string pivotColumn, IEnumerable<object> values) =>
            new RelationalGroupedDataset(
                (JvmObjectReference)Reference.Invoke("pivot", pivotColumn, values), _dataFrame);

        /// <summary>
        /// Pivots a column of the current DataFrame and performs the specified aggregation.
        /// </summary>
        /// <param name="pivotColumn">The column to pivot</param>
        /// <returns>New RelationalGroupedDataset object with pivot applied</returns>
        public RelationalGroupedDataset Pivot(Column pivotColumn) => 
            new RelationalGroupedDataset(
                (JvmObjectReference)Reference.Invoke("pivot", pivotColumn), _dataFrame);

        /// <summary>
        /// Pivots a column of the current DataFrame and performs the specified aggregation.
        /// </summary>
        /// <param name="pivotColumn">The column to pivot of type <see cref="Column"/></param>
        /// <param name="values">List of values that will be translated to columns in the
        /// output DataFrame.</param>
        /// <returns>New RelationalGroupedDataset object with pivot applied</returns>
        public RelationalGroupedDataset Pivot(Column pivotColumn, IEnumerable<object> values) =>
            new RelationalGroupedDataset(
                (JvmObjectReference)Reference.Invoke("pivot", pivotColumn, values), _dataFrame);

        /// <summary>
        /// Maps each group of the current DataFrame using a UDF and
        /// returns the result as a DataFrame.
        /// 
        /// The user-defined function should take an <see cref="FxDataFrame"/>
        /// and return another <see cref="FxDataFrame"/>. For each group, all
        /// columns are passed together as an <see cref="FxDataFrame"/> to the user-function and
        /// the returned FxDataFrame are combined as a DataFrame.
        ///
        /// The returned <see cref="FxDataFrame"/> can be of arbitrary length and its schema must
        /// match <paramref name="returnType"/>.
        /// </summary>
        /// <param name="returnType">
        /// The <see cref="StructType"/> that represents the schema of the return data set.
        /// </param>
        /// <param name="func">A grouped map user-defined function.</param>
        /// <returns>New DataFrame object with the UDF applied.</returns>
        public DataFrame Apply(StructType returnType, Func<FxDataFrame, FxDataFrame> func)
        {
            DataFrameGroupedMapWorkerFunction.ExecuteDelegate wrapper =
                new DataFrameGroupedMapUdfWrapper(func).Execute;

            var udf = UserDefinedFunction.Create(
                Reference.Jvm,
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

            return new DataFrame((JvmObjectReference)Reference.Invoke(
                "flatMapGroupsInPandas",
                udfColumn.Expr()));
        }

        /// <summary>
        /// Maps each group of the current DataFrame using a UDF and
        /// returns the result as a DataFrame.
        /// 
        /// The user-defined function should take an Apache Arrow RecordBatch
        /// and return another Apache Arrow RecordBatch. For each group, all
        /// columns are passed together as a RecordBatch to the user-function and
        /// the returned RecordBatch are combined as a DataFrame.
        ///
        /// The returned <see cref="RecordBatch"/> can be of arbitrary length and its
        /// schema must match <paramref name="returnType"/>.
        /// </summary>
        /// <param name="returnType">
        /// The <see cref="StructType"/> that represents the shape of the return data set.
        /// </param>
        /// <param name="func">A grouped map user-defined function.</param>
        /// <returns>New DataFrame object with the UDF applied.</returns>
        public DataFrame Apply(StructType returnType, Func<RecordBatch, RecordBatch> func)
        {
            ArrowGroupedMapWorkerFunction.ExecuteDelegate wrapper =
                new ArrowGroupedMapUdfWrapper(func).Execute;

            UserDefinedFunction udf = UserDefinedFunction.Create(
                Reference.Jvm,
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

            return new DataFrame((JvmObjectReference)Reference.Invoke(
                "flatMapGroupsInPandas",
                udfColumn.Expr()));
        }
    }
}
