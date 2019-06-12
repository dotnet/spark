// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Function that will be executed in the worker.
    /// </summary>
    internal abstract class WorkerFunction
    {
    }

    /// <summary>
    /// Function that will be executed in the worker using the Apache Arrow format.
    /// </summary>
    internal sealed class ArrowWorkerFunction : WorkerFunction
    {
        /// <summary>
        /// Type of the UDF to run. Refer to <see cref="ArrowUdfWrapper{T, T}"/>.Execute.
        /// </summary>
        /// <param name="input">unpickled data, representing a row</param>
        /// <param name="argOffsets">offsets to access input</param>
        /// <returns></returns>
        internal delegate IArrowArray ExecuteDelegate(
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets);

        internal ArrowWorkerFunction(ExecuteDelegate func)
        {
            Func = func;
        }

        internal ExecuteDelegate Func { get; }

        /// <summary>
        /// Used to chain functions.
        /// </summary>
        internal static ArrowWorkerFunction Chain(
            ArrowWorkerFunction innerWorkerFunction,
            ArrowWorkerFunction outerWorkerFunction)
        {
            return new ArrowWorkerFunction(
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

            internal IArrowArray Execute(
                ReadOnlyMemory<IArrowArray> input,
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
    internal sealed class ArrowGroupedMapWorkerFunction : WorkerFunction
    {
        /// <summary>
        /// A delegate to invoke a Grouped Map Vector UDF.
        /// </summary>
        /// <param name="input">The input data frame.</param>
        /// <returns>The resultant data frame.</returns>
        internal delegate RecordBatch ExecuteDelegate(RecordBatch input);

        internal ArrowGroupedMapWorkerFunction(ExecuteDelegate func)
        {
            Func = func;
        }

        internal ExecuteDelegate Func { get; }

        /// <summary>
        /// Used to chain functions.
        /// </summary>
        internal static ArrowGroupedMapWorkerFunction Chain(
            ArrowGroupedMapWorkerFunction innerWorkerFunction,
            ArrowGroupedMapWorkerFunction outerWorkerFunction)
        {
            return new ArrowGroupedMapWorkerFunction(
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

            internal RecordBatch Execute(RecordBatch input)
            {
                return _outerFunc(_innerFunc(input));
            }
        }
    }

    /// <summary>
    /// Function that will be executed in the worker using the Python pickling format.
    /// </summary>
    internal sealed class PicklingWorkerFunction : WorkerFunction
    {
        /// <summary>
        /// Type of the UDF to run. Refer to <see cref="PicklingUdfWrapper{T}"/>.Execute.
        /// </summary>
        /// <param name="splitId">split id for the current task</param>
        /// <param name="input">unpickled data, representing a row</param>
        /// <param name="argOffsets">offsets to access input</param>
        /// <returns></returns>
        internal delegate object ExecuteDelegate(int splitId, object[] input, int[] argOffsets);

        internal PicklingWorkerFunction(ExecuteDelegate func)
        {
            Func = func;
        }

        internal ExecuteDelegate Func { get; }

        /// <summary>
        /// Used to chain functions.
        /// </summary>
        internal static PicklingWorkerFunction Chain(
            PicklingWorkerFunction innerWorkerFunction,
            PicklingWorkerFunction outerWorkerFunction)
        {
            return new PicklingWorkerFunction(
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

            internal object Execute(int splitId, object input, int[] argOffsets)
            {
                // For chaining, create an array with one element, which is a result from the inner
                // function. Only the inner function will expect the given offsets, and the outer
                // function will always take 0 as an offset since there is only one value in the
                // input.
                return _outerFunc(
                    splitId,
                    new[] { _innerFunc(splitId, (object[])input, argOffsets) },
                    s_outerFuncArgOffsets);
            }
        }
    }
}
