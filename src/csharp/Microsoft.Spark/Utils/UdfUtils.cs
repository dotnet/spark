// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Utils
{
    using ArrowDelegate = ArrowWorkerFunction.ExecuteDelegate;
    using PicklingDelegate = PicklingWorkerFunction.ExecuteDelegate;

    /// <summary>
    /// UdfUtils provides UDF-related functions and enum.
    /// </summary>
    internal static class UdfUtils
    {
        /// <summary>
        /// Enum for Python evaluation type. This determines how the data will be serialized
        /// from Spark executor to its worker.
        /// Since UDF is based on PySpark implementation, PythonEvalType is used. Once
        /// generic interop layer is introduced, this will be revisited.
        /// This mirrors values defined in python/pyspark/rdd.py.
        /// </summary>
        internal enum PythonEvalType
        {
            NON_UDF = 0,

            SQL_BATCHED_UDF = 100,

            SQL_SCALAR_PANDAS_UDF = 200,
            SQL_GROUPED_MAP_PANDAS_UDF = 201,
            SQL_GROUPED_AGG_PANDAS_UDF = 202,
            SQL_WINDOW_AGG_PANDAS_UDF = 203
        }

        /// <summary>
        /// Mapping of supported types from .NET to org.apache.spark.sql.types.DataType in Scala.
        /// Refer to spark/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DataType.scala
        /// for more information.
        /// </summary>
        private static readonly Dictionary<Type, string> s_returnTypes =
            new Dictionary<Type, string>
            {
                {typeof(string), "string"},
                {typeof(byte[]), "binary"},
                {typeof(bool), "boolean"},
                {typeof(DateTime), "timestamp"},
                {typeof(decimal), "decimal(28,12)"},
                {typeof(double), "double"},
                {typeof(float), "float"},
                {typeof(byte), "tinyint"},
                {typeof(int), "integer"},
                {typeof(long), "bigint"},
                {typeof(short), "smallint"}
            };

        /// <summary>
        /// Returns the return type of an UDF in JSON format. This value is used to
        /// create a org.apache.spark.sql.types.DataType object from JSON string.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        internal static string GetReturnType(Type type)
        {
            // See spark.sql.datatypes.datatype.scala for reference.
            var types = new Type[] { type };
            var returnTypeFormat = "{0}";

            if (type.IsArray)
            {
                types[0] = type.GetElementType();
                returnTypeFormat = "array<{0}>";
            }
            else if (type.IsGenericType &&
                (type.GetGenericTypeDefinition() == typeof(Dictionary<,>)))
            {
                types = type.GenericTypeArguments;
                returnTypeFormat = "map<{0},{1}>";
            }

            if (types.Any(t => !s_returnTypes.ContainsKey(t)))
            {
                throw new ArgumentException(
                    $"{type.Name} not supported. Supported types: {string.Join(",", s_returnTypes.Keys)}");
            }

            return string.Format(returnTypeFormat, types.Select(t => s_returnTypes[t]).ToArray());
        }

        /// <summary>
        /// Creates the PythonFunction object on the JVM side wrapping the given command bytes.
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        /// <param name="command">Serialized command bytes</param>
        /// <returns>JvmObjectReference object to the PythonFunction object</returns>
        internal static JvmObjectReference CreatePythonFunction(IJvmBridge jvm, byte[] command)
        {
            JvmObjectReference hashTableReference = jvm.CallConstructor("java.util.Hashtable");
            JvmObjectReference arrayListReference = jvm.CallConstructor("java.util.ArrayList");

            return (JvmObjectReference)jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.api.dotnet.SQLUtils",
                "createPythonFunction",
                command,
                hashTableReference, // Environment variables
                arrayListReference, // Python includes
                SparkEnvironment.ConfigurationService.GetWorkerExePath(),
                "1.0",
                arrayListReference, // Broadcast variables
                null); // Accumulator
        }

        private static readonly bool s_useArrow =
            EnvironmentUtils.GetEnvironmentVariableAsBool("SPARK_DOTNET_USE_ARROW_UDF");

        internal static PythonEvalType GetPythonEvalType()
        {
            return s_useArrow ?
                PythonEvalType.SQL_SCALAR_PANDAS_UDF :
                PythonEvalType.SQL_BATCHED_UDF;
        }

        internal static Delegate CreateUdfWrapper<TResult>(Func<TResult> udf)
        {

            if (s_useArrow)
            {
                return (ArrowDelegate)new ArrowUdfWrapper<TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)new PicklingUdfWrapper<TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, TResult>(Func<T1, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)new ArrowUdfWrapper<T1, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)new PicklingUdfWrapper<T1, TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, T2, TResult>(Func<T1, T2, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)new ArrowUdfWrapper<T1, T2, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)new PicklingUdfWrapper<T1, T2, TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, TResult>(
            Func<T1, T2, T3, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)new ArrowUdfWrapper<T1, T2, T3, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)new PicklingUdfWrapper<T1, T2, T3, TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, TResult>(
            Func<T1, T2, T3, T4, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)new ArrowUdfWrapper<T1, T2, T3, T4, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)
                    new PicklingUdfWrapper<T1, T2, T3, T4, TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, TResult>(
            Func<T1, T2, T3, T4, T5, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)
                    new ArrowUdfWrapper<T1, T2, T3, T4, T5, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)
                    new PicklingUdfWrapper<T1, T2, T3, T4, T5, TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, TResult>(
            Func<T1, T2, T3, T4, T5, T6, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)
                    new ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)
                    new PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)
                    new ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)
                    new PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)
                    new ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)
                    new PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)
                    new ArrowUdfWrapper<
                        T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)
                    new PicklingUdfWrapper<
                        T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(udf).Execute;
            }
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> udf)
        {
            if (s_useArrow)
            {
                return (ArrowDelegate)
                    new ArrowUdfWrapper<
                        T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(udf).Execute;
            }
            else
            {
                return (PicklingDelegate)
                    new PicklingUdfWrapper<
                        T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(udf).Execute;
            }
        }
    }
}
