// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// A helper interface for JavaModel, so that when we have an array of JavaModels
    /// with different type params, we can hold all of them with Model&lt;object&gt;.
    /// </summary>
    public interface IModel<out M>
    {
        bool HasParent();
    }

    /// <summary>
    /// A fitted model, i.e., a Transformer produced by an Estimator.
    /// </summary>
    /// <typeparam name="M">
    /// Model Type.
    /// </typeparam>
    public abstract class JavaModel<M> : JavaTransformer, IModel<M> where M : JavaModel<M>
    {
        internal JavaModel(string className) : base(className)
        {
        }

        internal JavaModel(string className, string uid) : base(className, uid)
        {
        }

        internal JavaModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets the parent of this model.
        /// </summary>
        /// <param name="parent">The parent of the JavaModel to be set</param>
        /// <returns>type parameter M</returns>
        public M SetParent(JavaEstimator<M> parent) =>
            WrapAsType<M>((JvmObjectReference)Reference.Invoke("setParent", parent));

        /// <summary>
        /// Indicates whether this Model has a corresponding parent.
        /// </summary>
        /// <returns>bool</returns>
        public bool HasParent() =>
            (bool)Reference.Invoke("hasParent");
    }
}
