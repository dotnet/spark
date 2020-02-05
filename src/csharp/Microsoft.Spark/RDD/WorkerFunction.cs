// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;

namespace Microsoft.Spark.RDD
{
    /// <summary>
    /// WorkerFunction provides the delegate type that is used for unifying
    /// UDFs used for RDD. It also provides functionality to chain delegates.
    /// </summary>
    internal sealed class WorkerFunction
    {
        /// <summary>
        /// Delegate type to which each RDD UDF is transformed.
        /// </summary>
        /// <param name="splitId">split id for the current task</param>
        /// <param name="input">enumerable collection of objects</param>
        /// <returns>enumerable collection of objects after applying UDF</returns>
        internal delegate IEnumerable<object> ExecuteDelegate(
            int splitId,
            IEnumerable<object> input);

        public WorkerFunction(ExecuteDelegate func)
        {
            Func = func;
        }

        internal ExecuteDelegate Func { get; }

        /// <summary>
        /// Used to chain two function.
        /// </summary>
        internal static WorkerFunction Chain(
            WorkerFunction innerFunction,
            WorkerFunction outerFunction)
        {
            return new WorkerFunction(
                new WorkerFuncChainHelper(
                    innerFunction.Func,
                    outerFunction.Func).Execute);
        }

        /// <summary>
        /// Helper to chain two delegates.
        /// </summary>
        [UdfWrapper]
        private sealed class WorkerFuncChainHelper
        {
            private readonly ExecuteDelegate _innerFunc;
            private readonly ExecuteDelegate _outerFunc;

            internal WorkerFuncChainHelper(ExecuteDelegate innerFunc, ExecuteDelegate outerFunc)
            {
                _innerFunc = innerFunc;
                _outerFunc = outerFunc;
            }

            internal IEnumerable<object> Execute(int split, IEnumerable<object> input)
            {
                return _outerFunc(split, _innerFunc(split, input));
            }
        }
    }
}
