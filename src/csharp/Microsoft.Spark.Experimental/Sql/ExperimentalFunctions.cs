// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Experimental functions available for DataFrame operations.
    /// </summary>
    /// <remarks>
    /// These APIs are subject to change in future versions.
    /// </remarks>
    public static class ExperimentalFunctions
    {
        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column> VectorUdf<T1, RT>(Func<T1, RT> udf)
            where T1 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column> VectorUdf<T1, T2, RT>(Func<T1, T2, RT> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column> VectorUdf<T1, T2, T3, RT>(
            Func<T1, T2, T3, RT> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, RT>(
            Func<T1, T2, T3, T4, RT> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, RT>(
            Func<T1, T2, T3, T4, T5, RT> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, RT>(
            Func<T1, T2, T3, T4, T5, T6, RT> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, T7, RT>(
            Func<T1, T2, T3, T4, T5, T6, T7, RT> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where T7 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
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
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, T7, T8, RT>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, RT> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where T7 : IArrowArray
            where T8 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
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
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, T7, T8, T9, RT>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, RT> udf)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where T3 : IArrowArray
            where T4 : IArrowArray
            where T5 : IArrowArray
            where T6 : IArrowArray
            where T7 : IArrowArray
            where T8 : IArrowArray
            where T9 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
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
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, RT>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, RT> udf)
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
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }
    }
}
