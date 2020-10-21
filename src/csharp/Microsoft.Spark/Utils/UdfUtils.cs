// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Utils
{
    using ArrowDelegate = ArrowWorkerFunction.ExecuteDelegate;
    using PicklingDelegate = PicklingWorkerFunction.ExecuteDelegate;

    /// <summary>
    /// UdfTypeUtils provides functions related to UDF types.
    /// </summary>
    internal static class UdfTypeUtils
    {
        /// <summary>
        /// Returns "true" if the given type is nullable.
        /// </summary>
        /// <param name="type">Type to check if it is nullable</param>
        /// <returns>"true" if the given type is nullable. Otherwise, returns "false"</returns>
        internal static string CanBeNull(this Type type)
        {
            return (!type.IsValueType || (Nullable.GetUnderlyingType(type) != null)) ?
                "true" :
                "false";
        }

        /// <summary>
        /// Returns the generic type definition of a given type if the given type is equal
        /// to or implements the `compare` type. Returns null if there is no match.
        /// </summary>
        /// <param name="type">This type object</param>
        /// <param name="compare">Generic type definition to compare to</param>
        /// <returns>Matching generic type object</returns>
        internal static Type ImplementsGenericTypeOf(this Type type, Type compare)
        {
            Debug.Assert(compare.IsGenericType);
            return (type.IsGenericType && (type.GetGenericTypeDefinition() == compare)) ?
                type :
                type.GetInterface(compare.FullName);
        }
    }

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
            SQL_WINDOW_AGG_PANDAS_UDF = 203,

            SQL_SCALAR_PANDAS_ITER_UDF = 204,
            SQL_MAP_PANDAS_ITER_UDF = 205,
            SQL_COGROUPED_MAP_PANDAS_UDF = 206
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
                {typeof(decimal), "decimal(28,12)"},
                {typeof(double), "double"},
                {typeof(float), "float"},
                {typeof(byte), "byte"},
                {typeof(int), "integer"},
                {typeof(long), "long"},
                {typeof(short), "short"},
                {typeof(Date), "date"},
                {typeof(Timestamp), "timestamp"},

                // Arrow array types
                {typeof(BooleanArray), "boolean"},
                {typeof(UInt8Array), "byte"},
                {typeof(Int16Array), "short"},
                {typeof(Int32Array), "integer"},
                {typeof(Int64Array), "long"},
                {typeof(FloatArray), "float"},
                {typeof(DoubleArray), "double"},
                {typeof(StringArray), "string"},
                {typeof(BinaryArray), "binary"},

                {typeof(BooleanDataFrameColumn), "boolean"},
                {typeof(ByteDataFrameColumn), "byte"},
                {typeof(Int16DataFrameColumn), "short"},
                {typeof(Int32DataFrameColumn), "integer"},
                {typeof(Int64DataFrameColumn), "long"},
                {typeof(SingleDataFrameColumn), "float"},
                {typeof(DoubleDataFrameColumn), "double"},
                {typeof(ArrowStringDataFrameColumn), "string"},
            };

        /// <summary>
        /// Returns the return type of an UDF in JSON format. This value is used to
        /// create a org.apache.spark.sql.types.DataType object from JSON string.
        /// </summary>
        /// <param name="type">Return type of an UDF</param>
        /// <returns>JSON format of the return type</returns>
        internal static string GetReturnType(Type type)
        {
            if (s_returnTypes.TryGetValue(type, out string value))
            {
                return $@"""{value}""";
            }

            Type dictionaryType = type.ImplementsGenericTypeOf(typeof(IDictionary<,>));
            if (dictionaryType != null)
            {
                Type[] typeArguments = dictionaryType.GenericTypeArguments;
                Type keyType = typeArguments[0];
                Type valueType = typeArguments[1];
                return @"{""type"":""map"", " +
                    $@"""keyType"":{GetReturnType(keyType)}, " +
                    $@"""valueType"":{GetReturnType(valueType)}, " +
                    $@"""valueContainsNull"":{valueType.CanBeNull()}}}";
            }

            Type enumerableType = type.ImplementsGenericTypeOf(typeof(IEnumerable<>));
            if (enumerableType != null)
            {
                Type elementType = enumerableType.GenericTypeArguments[0];
                return @"{""type"":""array"", " +
                    $@"""elementType"":{GetReturnType(elementType)}, " +
                    $@"""containsNull"":{elementType.CanBeNull()}}}";
            }

            throw new ArgumentException($"{type.FullName} is not supported.");
        }

        /// <summary>
        /// Creates the PythonFunction object on the JVM side wrapping the given command bytes.
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        /// <param name="command">Serialized command bytes</param>
        /// <returns>JvmObjectReference object to the PythonFunction object</returns>
        internal static JvmObjectReference CreatePythonFunction(IJvmBridge jvm, byte[] command)
        {
            var arrayList = new ArrayList(jvm);
            var broadcastVariables = new ArrayList(jvm);
            broadcastVariables.AddAll(JvmBroadcastRegistry.GetAll());
            JvmBroadcastRegistry.Clear();

            return (JvmObjectReference)jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.api.dotnet.SQLUtils",
                "createPythonFunction",
                command,
                CreateEnvVarsForPythonFunction(jvm),
                arrayList, // Python includes
                SparkEnvironment.ConfigurationService.GetWorkerExePath(),
                // Used to check the compatibility of UDFs between the driver and worker.
                AssemblyInfoProvider.MicrosoftSparkAssemblyInfo().AssemblyVersion,
                broadcastVariables,
                null); // Accumulator
        }

        private static IJvmObjectReferenceProvider CreateEnvVarsForPythonFunction(IJvmBridge jvm)
        {
            var environmentVars = new Hashtable(jvm);
            string assemblySearchPath = Environment.GetEnvironmentVariable(
                AssemblySearchPathResolver.AssemblySearchPathsEnvVarName);
            if (!string.IsNullOrEmpty(assemblySearchPath))
            {
                environmentVars.Put(
                    AssemblySearchPathResolver.AssemblySearchPathsEnvVarName,
                    assemblySearchPath);
            }
            // DOTNET_WORKER_SPARK_VERSION is used to handle different versions
            // of Spark on the worker.
            environmentVars.Put(
                "DOTNET_WORKER_SPARK_VERSION",
                SparkEnvironment.SparkVersion.ToString());

            if (SparkEnvironment.ConfigurationService.IsRunningRepl())
            {
                environmentVars.Put(Constants.RunningREPLEnvVar, "true");
            }

            return environmentVars;
        }

        internal static Delegate CreateUdfWrapper<TResult>(Func<TResult> udf)
        {
            return (PicklingDelegate)new PicklingUdfWrapper<TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T, TResult>(Func<T, TResult> udf)
        {
            return (PicklingDelegate)new PicklingUdfWrapper<T, TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T1, T2, TResult>(Func<T1, T2, TResult> udf)
        {
            return (PicklingDelegate)new PicklingUdfWrapper<T1, T2, TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, TResult>(
            Func<T1, T2, T3, TResult> udf)
        {
            return (PicklingDelegate)new PicklingUdfWrapper<T1, T2, T3, TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, TResult>(
            Func<T1, T2, T3, T4, TResult> udf)
        {
            return (PicklingDelegate)
                new PicklingUdfWrapper<T1, T2, T3, T4, TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, TResult>(
            Func<T1, T2, T3, T4, T5, TResult> udf)
        {
            return (PicklingDelegate)
                new PicklingUdfWrapper<T1, T2, T3, T4, T5, TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, TResult>(
            Func<T1, T2, T3, T4, T5, T6, TResult> udf)
        {
            return (PicklingDelegate)
                new PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, TResult> udf)
        {
            return (PicklingDelegate)
                new PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> udf)
        {
            return (PicklingDelegate)
                new PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> udf)
        {
            return (PicklingDelegate)
                new PicklingUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(udf).Execute;
        }

        internal static Delegate CreateUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> udf)
        {
            return (PicklingDelegate)
                new PicklingUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T, TResult>(Func<T, TResult> udf)
            where T : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)new ArrowUdfWrapper<T, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, TResult>(Func<T1, T2, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)new ArrowUdfWrapper<T1, T2, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, TResult>(
            Func<T1, T2, T3, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)new ArrowUdfWrapper<T1, T2, T3, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, TResult>(
            Func<T1, T2, T3, T4, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)
                new ArrowUdfWrapper<T1, T2, T3, T4, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, TResult>(
            Func<T1, T2, T3, T4, T5, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)
                new ArrowUdfWrapper<T1, T2, T3, T4, T5, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, TResult>(
            Func<T1, T2, T3, T4, T5, T6, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)
                new ArrowUdfWrapper<
                    T1, T2, T3, T4, T5, T6, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where T7 : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)
                new ArrowUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where T7 : IArrowArray
            where T8 : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)
                new ArrowUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, T8, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where T7 : IArrowArray
            where T8 : IArrowArray
            where T9 : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)
                new ArrowUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where T7 : IArrowArray
            where T8 : IArrowArray
            where T9 : IArrowArray
            where T10 : IArrowArray
            where TResult : IArrowArray
        {
            return (ArrowDelegate)
                new ArrowUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(udf).Execute;
        }
    }
}
