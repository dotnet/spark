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
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column> VectorUdf<A1, RT>(Func<A1, RT> udf)
            where A1 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="A2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column> VectorUdf<A1, A2, RT>(Func<A1, A2, RT> udf)
            where A1 : IArrowArray
            where A2 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="A2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="A3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column> VectorUdf<A1, A2, A3, RT>(
            Func<A1, A2, A3, RT> udf)
            where A1 : IArrowArray
            where A2 : IArrowArray
            where A3 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="A2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="A3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="A4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column> VectorUdf<A1, A2, A3, A4, RT>(
            Func<A1, A2, A3, A4, RT> udf)
            where A1 : IArrowArray
            where A2 : IArrowArray
            where A3 : IArrowArray
            where A4 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="A2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="A3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="A4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="A5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column> VectorUdf<A1, A2, A3, A4, A5, RT>(
            Func<A1, A2, A3, A4, A5, RT> udf)
            where A1 : IArrowArray
            where A2 : IArrowArray
            where A3 : IArrowArray
            where A4 : IArrowArray
            where A5 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="A2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="A3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="A4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="A5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="A6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column> VectorUdf<A1, A2, A3, A4, A5, A6, RT>(
            Func<A1, A2, A3, A4, A5, A6, RT> udf)
            where A1 : IArrowArray
            where A2 : IArrowArray
            where A3 : IArrowArray
            where A4 : IArrowArray
            where A5 : IArrowArray
            where A6 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="A2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="A3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="A4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="A5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="A6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="A7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<A1, A2, A3, A4, A5, A6, A7, RT>(
            Func<A1, A2, A3, A4, A5, A6, A7, RT> udf)
            where A1 : IArrowArray
            where A2 : IArrowArray
            where A3 : IArrowArray
            where A4 : IArrowArray
            where A5 : IArrowArray
            where A6 : IArrowArray
            where A7 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="A2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="A3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="A4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="A5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="A6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="A7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="A8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<A1, A2, A3, A4, A5, A6, A7, A8, RT>(
            Func<A1, A2, A3, A4, A5, A6, A7, A8, RT> udf)
            where A1 : IArrowArray
            where A2 : IArrowArray
            where A3 : IArrowArray
            where A4 : IArrowArray
            where A5 : IArrowArray
            where A6 : IArrowArray
            where A7 : IArrowArray
            where A8 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="A2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="A3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="A4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="A5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="A6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="A7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="A8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="A9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT>(
            Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> udf)
            where A1 : IArrowArray
            where A2 : IArrowArray
            where A3 : IArrowArray
            where A4 : IArrowArray
            where A5 : IArrowArray
            where A6 : IArrowArray
            where A7 : IArrowArray
            where A8 : IArrowArray
            where A9 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }

        /// <summary>Creates a Vector UDF from the specified delegate.</summary>
        /// <typeparam name="A1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="A2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="A3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="A4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="A5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="A6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="A7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="A8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="A9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <typeparam name="A10">Specifies the type of the tenth argument to the UDF.</typeparam>
        /// <typeparam name="RT">Specifies the return type of the UDF.</typeparam>
        /// <param name="udf">The Vector UDF function implementation.</param>
        /// <returns>
        /// A delegate that returns a <see cref="Column"/> for the result of the Vector UDF.
        /// </returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> VectorUdf<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT>(
            Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> udf)
            where A1 : IArrowArray
            where A2 : IArrowArray
            where A3 : IArrowArray
            where A4 : IArrowArray
            where A5 : IArrowArray
            where A6 : IArrowArray
            where A7 : IArrowArray
            where A8 : IArrowArray
            where A9 : IArrowArray
            where A10 : IArrowArray
            where RT : IArrowArray
        {
            return Functions.VectorUdf(udf);
        }
    }
}
