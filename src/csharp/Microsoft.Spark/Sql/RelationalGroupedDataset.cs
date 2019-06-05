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
            // TODO: func needs to change to be our *UdfWrapper
            byte[] command = CommandSerDe.Serialize(
                func,
                CommandSerDe.SerializedMode.Row,
                CommandSerDe.SerializedMode.Row);

            JvmObjectReference pythonFunction =
                UdfUtils.CreatePythonFunction(_jvmObject.Jvm, command);

            var udf = new UserDefinedFunction(
                _jvmObject.Jvm.CallConstructor(
                    "org.apache.spark.sql.execution.python.UserDefinedPythonFunction",
                    name,
                    pythonFunction,
                    GetDataType(returnType),
                    (int)UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                    true // udfDeterministic
                    ));

            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("flatMapGroupsInPandas", udf));
        }

        private JvmObjectReference GetDataType(StructType type)
        {
            return (JvmObjectReference)_jvmObject.Jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.types.DataType",
                "fromJson",
                type.Json);
        }
    }
}
