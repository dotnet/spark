// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Functions available for DataFrame operations.
    /// </summary>
    public static class ArrowFunctions
    {
        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column> VectorUdf<T, TResult>(Func<T, TResult> udf)
            where T : IArrowArray
            where TResult : IArrowArray
        {
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply1;
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column> VectorUdf<T1, T2, TResult>(Func<T1, T2, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where TResult : IArrowArray
        {
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply2;
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column> VectorUdf<T1, T2, T3, TResult>(
            Func<T1, T2, T3, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where TResult : IArrowArray
        {
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply3;
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, TResult>(
            Func<T1, T2, T3, T4, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where TResult : IArrowArray
        {
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply4;
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, TResult>(
            Func<T1, T2, T3, T4, T5, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where TResult : IArrowArray
        {
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply5;
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, TResult>(
            Func<T1, T2, T3, T4, T5, T6, TResult> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where TResult : IArrowArray
        {
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply6;
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, T7, TResult>(
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
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply7;
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(
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
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply8;
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
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
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
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
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply9;
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
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
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
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
            return Functions.CreateVectorUdf<TResult>(
                udf.Method.ToString(),
                UdfUtils.CreateVectorUdfWrapper(udf)).Apply10;
        }
    }
}
