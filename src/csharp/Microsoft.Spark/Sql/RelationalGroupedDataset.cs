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

        internal DataFrame Apply(StructType returnType, Func<RecordBatch, RecordBatch> func)
        {
            var udf = UserDefinedFunction.Create(
                _jvmObject.Jvm,
                func.Method.ToString(),
                CommandSerDe.Serialize(
                    func,
                    CommandSerDe.SerializedMode.Row,
                    CommandSerDe.SerializedMode.Row),
                UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                returnType.Json);

            IReadOnlyList<string> columnNames = _dataFrame.Columns();
            Column[] columns = new Column[columnNames.Count];
            for (int i = 0; i< columnNames.Count; ++i)
            {
                columns[i] = _dataFrame[columnNames[i]];
            }

            Column udf_column = udf.Apply(columns);

            return new DataFrame((JvmObjectReference)_jvmObject.Invoke(
                "flatMapGroupsInPandas",
                udf_column.Expr()));
        }
    }
}
