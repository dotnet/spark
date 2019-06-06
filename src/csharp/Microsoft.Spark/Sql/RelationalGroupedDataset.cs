// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
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

        internal DataFrame Apply(string name, StructType returnType, Func<RecordBatch, RecordBatch> func)
        {
            ArrowWorkerFunction.GroupedMapExecuteDelegate wrapper = new ArrowGroupedMapUdfWrapper(func).Execute;

            var udf = UserDefinedFunction.Create(
                func.Method.ToString(),
                CommandSerDe.Serialize(
                    wrapper,
                    CommandSerDe.SerializedMode.Row,
                    CommandSerDe.SerializedMode.Row),
                UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                returnType.Json);

            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("flatMapGroupsInPandas", udf));
        }
    }
}
