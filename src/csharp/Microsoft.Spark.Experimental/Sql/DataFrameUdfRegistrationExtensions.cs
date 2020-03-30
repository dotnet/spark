// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Extension methods for <see cref="UdfRegistration"/>.
    /// </summary>
    public static class DataFrameUdfRegistrationExtensions
    {
        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T, TResult>(
            this UdfRegistration udf, string name, Func<T, TResult> f)
            where T : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T1, T2, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, TResult> f)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T1, T2, T3, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, T3, TResult> f)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T1, T2, T3, T4, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, T3, T4, TResult> f)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T1, T2, T3, T4, T5, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, T3, T4, T5, TResult> f)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where T5 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T1, T2, T3, T4, T5, T6, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, T3, T4, T5, T6, TResult> f)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where T5 : DataFrameColumn
            where T6 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T1, T2, T3, T4, T5, T6, T7, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, T3, T4, T5, T6, T7, TResult> f)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where T5 : DataFrameColumn
            where T6 : DataFrameColumn
            where T7 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> f)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where T5 : DataFrameColumn
            where T6 : DataFrameColumn
            where T7 : DataFrameColumn
            where T8 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="T9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> f)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where T5 : DataFrameColumn
            where T6 : DataFrameColumn
            where T7 : DataFrameColumn
            where T8 : DataFrameColumn
            where T9 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="T9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <typeparam name="T10">Specifies the type of the tenth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public static void RegisterVector<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> f)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where T5 : DataFrameColumn
            where T6 : DataFrameColumn
            where T7 : DataFrameColumn
            where T8 : DataFrameColumn
            where T9 : DataFrameColumn
            where T10 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            RegisterVector<TResult>(udf, name, DataFrameUdfUtils.CreateVectorUdfWrapper(f));
        }

        private static void RegisterVector<TResult>(UdfRegistration udf, string name, Delegate func)
        {
            udf.Register<TResult>(name, func, UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF);
        }
    }
}
