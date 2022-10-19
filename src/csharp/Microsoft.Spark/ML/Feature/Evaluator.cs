// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// <see cref="JavaEvaluator"/> Abstract Class for evaluators that compute metrics from predictions.
    /// </summary>
    public abstract class JavaEvaluator : Params
    {
        internal JavaEvaluator(string className) : base(className)
        {
        }

        internal JavaEvaluator(string className, string uid) : base(className, uid)
        {
        }

        internal JavaEvaluator(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Evaluates model output and returns a scalar metric.
        /// The value of isLargerBetter specifies whether larger values are better.
        /// </summary>
        /// <param name="dataset">a dataset that contains labels/observations and predictions.</param>
        /// <returns>metric</returns>
        public virtual double Evaluate(DataFrame dataset) =>
            (double)Reference.Invoke("evaluate", dataset);

        /// <summary>
        /// Indicates whether the metric returned by evaluate should be maximized 
        /// (true, default) or minimized (false).
        /// A given evaluator may support multiple metrics which may be maximized or minimized.
        /// </summary>
        /// <returns>bool</returns>
        public bool IsLargerBetter =>
            (bool)Reference.Invoke("isLargerBetter");
    }
}
