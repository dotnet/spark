// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Utils
{
    using DataFrameDelegate = DataFrameWorkerFunction.ExecuteDelegate;

    /// <summary>
    /// DataFrameUdfUtils provides utility functions to wrap UDFs that use Microsoft.Data.Analysis
    /// </summary>
    internal static class DataFrameUdfUtils
    {
        internal static Delegate CreateVectorUdfWrapper<T, TResult>(Func<T, TResult> udf)
            where T : DataFrameColumn
            where TResult : DataFrameColumn
        {
            return (DataFrameDelegate)new DataFrameUdfWrapper<T, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, TResult>(Func<T1, T2, TResult> udf)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            return (DataFrameDelegate)new DataFrameUdfWrapper<T1, T2, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, TResult>(
            Func<T1, T2, T3, TResult> udf)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            return (DataFrameDelegate)new DataFrameUdfWrapper<T1, T2, T3, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, TResult>(
            Func<T1, T2, T3, T4, TResult> udf)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            return (DataFrameDelegate)
                new DataFrameUdfWrapper<T1, T2, T3, T4, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, TResult>(
            Func<T1, T2, T3, T4, T5, TResult> udf)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where T5 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            return (DataFrameDelegate)
                new DataFrameUdfWrapper<T1, T2, T3, T4, T5, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, TResult>(
            Func<T1, T2, T3, T4, T5, T6, TResult> udf)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where T5 : DataFrameColumn
            where T6 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            return (DataFrameDelegate)
                new DataFrameUdfWrapper<
                    T1, T2, T3, T4, T5, T6, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, TResult> udf)
            where T1 : DataFrameColumn
            where T2 : DataFrameColumn
            where T3 : DataFrameColumn
            where T4 : DataFrameColumn
            where T5 : DataFrameColumn
            where T6 : DataFrameColumn
            where T7 : DataFrameColumn
            where TResult : DataFrameColumn
        {
            return (DataFrameDelegate)
                new DataFrameUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> udf)
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
            return (DataFrameDelegate)
                new DataFrameUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, T8, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> udf)
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
            return (DataFrameDelegate)
                new DataFrameUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(udf).Execute;
        }

        internal static Delegate CreateVectorUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> udf)
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
            return (DataFrameDelegate)
                new DataFrameUdfWrapper<
                    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(udf).Execute;
        }
    }
}
