// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;

namespace Microsoft.Spark.Sql
{
    ///// <summary>
    ///// Function that will be executed in the worker.
    ///// </summary>
    //internal abstract class WorkerFunction
    //{
    //}

    /// <summary>
    /// Function that will be executed in the worker using the Apache Arrow format.
    /// </summary>
    internal sealed class DataFrameWorkerFunction : WorkerFunction
    {
        /// <summary>
        /// Type of the UDF to run. Refer to <see cref="ArrowUdfWrapper{T, T}"/>.Execute.
        /// </summary>
        /// <param name="input">unpickled data, representing a row</param>
        /// <param name="argOffsets">offsets to access input</param>
        /// <returns></returns>
        internal delegate DataFrameColumn ExecuteDelegate(
            ReadOnlyMemory<DataFrameColumn> input,
            int[] argOffsets);

        internal DataFrameWorkerFunction(ExecuteDelegate func)
        {
            Func = func;
        }

        internal ExecuteDelegate Func { get; }

        /// <summary>
        /// Used to chain functions.
        /// </summary>
        internal static DataFrameWorkerFunction Chain(
            DataFrameWorkerFunction innerWorkerFunction,
            DataFrameWorkerFunction outerWorkerFunction)
        {
            return new DataFrameWorkerFunction(
                new WorkerFuncChainHelper(
                    innerWorkerFunction.Func,
                    outerWorkerFunction.Func).Execute);
        }

        private class WorkerFuncChainHelper
        {
            private readonly ExecuteDelegate _innerFunc;
            private readonly ExecuteDelegate _outerFunc;

            /// <summary>
            /// The outer function will always take 0 as an offset since there is only one
            /// return value from an inner function.
            /// </summary>
            private static readonly int[] s_outerFuncArgOffsets = { 0 };

            internal WorkerFuncChainHelper(ExecuteDelegate inner, ExecuteDelegate outer)
            {
                _innerFunc = inner;
                _outerFunc = outer;
            }

            internal DataFrameColumn Execute(
                ReadOnlyMemory<DataFrameColumn> input,
                int[] argOffsets)
            {
                // For chaining, create an array with one element, which is a result from the inner
                // function. Only the inner function will expect the given offsets, and the outer
                // function will always take 0 as an offset since there is only one value in the
                // input.
                return _outerFunc(
                   new[] { _innerFunc(input, argOffsets) },
                   s_outerFuncArgOffsets);
            }
        }
    }

    /// <summary>
    /// Function for Grouped Map Vector UDFs using the Apache Arrow format.
    /// </summary>
    internal sealed class DataFrameGroupedMapWorkerFunction : WorkerFunction
    {
        /// <summary>
        /// A delegate to invoke a Grouped Map Vector UDF.
        /// </summary>
        /// <param name="input">The input data frame.</param>
        /// <returns>The resultant data frame.</returns>
        internal delegate FxDataFrame ExecuteDelegate(FxDataFrame input);

        internal DataFrameGroupedMapWorkerFunction(ExecuteDelegate func)
        {
            Func = func;
        }

        internal ExecuteDelegate Func { get; }
    }
}
