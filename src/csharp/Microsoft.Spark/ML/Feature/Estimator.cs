// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// A helper interface for JavaEstimator, so that when we have an array of JavaEstimators
    /// with different type params, we can hold all of them with Estimator&lt;object&gt;.
    /// </summary>
    public interface IEstimator<out M>
    {
        M Fit(DataFrame dataset);
    }

    /// <summary>
    /// Abstract Class for estimators that fit models to data.
    /// </summary>
    /// <typeparam name="M"/>
    public abstract class JavaEstimator<M> : JavaPipelineStage, IEstimator<M> where M : JavaModel<M>
    {
        internal JavaEstimator(string className) : base(className)
        {
        }

        internal JavaEstimator(string className, string uid) : base(className, uid)
        {
        }

        internal JavaEstimator(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Fits a model to the input data.
        /// </summary>
        /// <param name="dataset">input dataset.</param>
        /// <returns>fitted model</returns>
        public virtual M Fit(DataFrame dataset) =>
            WrapAsType<M>((JvmObjectReference)Reference.Invoke("fit", dataset));
    }
}
