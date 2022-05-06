// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;
using System.Collections.Generic;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// <see cref="Pipeline"/> A simple pipeline, which acts as an estimator. 
    /// A Pipeline consists of a sequence of stages, each of which is either an Estimator or a Transformer.
    /// When Pipeline.fit is called, the stages are executed in order. If a stage is an Estimator, its 
    /// Estimator.fit method will be called on the input dataset to fit a model. Then the model, which is a 
    /// transformer, will be used to transform the dataset as the input to the next stage. 
    /// If a stage is a Transformer, its Transformer.transform method will be called to produce the 
    /// dataset for the next stage. The fitted model from a Pipeline is a PipelineModel, which consists of 
    /// fitted models and transformers, corresponding to the pipeline
    /// stages. If there are no stages, the pipeline acts as an identity transformer.
    /// </summary>
    public class Pipeline :
        JavaEstimator<PipelineModel>,
        IJavaMLWritable,
        IJavaMLReadable<Pipeline>
    {
        private static readonly string s_className = "org.apache.spark.ml.Pipeline";

        /// <summary>
        /// Creates a <see cref="Pipeline"/> without any parameters.
        /// </summary>
        public Pipeline() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="Pipeline"/> with a UID that is used to give the
        /// <see cref="Pipeline"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Pipeline(string uid) : base(s_className, uid)
        {
        }

        internal Pipeline(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Set the stages of pipeline instance.
        /// </summary>
        /// <param name="value">
        /// A sequence of stages, each of which is either an Estimator or a Transformer.
        /// </param>
        /// <returns><see cref="Pipeline"/> object</returns>
        public Pipeline SetStages(JavaPipelineStage[] value) =>
            WrapAsPipeline((JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "org.apache.spark.mllib.api.dotnet.MLUtils",
                "setPipelineStages",
                Reference,
                value.ToJavaArrayList()));

        /// <summary>
        /// Get the stages of pipeline instance.
        /// </summary>
        /// <returns>A sequence of <see cref="JavaPipelineStage"/> stages</returns>
        public JavaPipelineStage[] GetStages()
        {
            var jvmObjects = (JvmObjectReference[])Reference.Invoke("getStages");
            var result = new JavaPipelineStage[jvmObjects.Length];
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(JavaPipelineStage),
                "s_className");

            for (int i = 0; i < jvmObjects.Length; i++)
            {
                if (JvmObjectUtils.TryConstructInstanceFromJvmObject(
                    jvmObjects[i],
                    classMapping,
                    out JavaPipelineStage instance))
                {
                    result[i] = instance;
                }                   
            }

            return result;
        }

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="PipelineModel"/></returns>
        override public PipelineModel Fit(DataFrame dataset) =>
            new PipelineModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));

        /// <summary>
        /// Loads the <see cref="Pipeline"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="Pipeline"/> was saved to</param>
        /// <returns>New <see cref="Pipeline"/> object, loaded from path.</returns>
        public static Pipeline Load(string path) => WrapAsPipeline(
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(s_className, "load", path));

        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        public void Save(string path) => Reference.Invoke("save", path);

        /// <summary>
        /// Get the corresponding JavaMLWriter instance.
        /// </summary>
        /// <returns>a <see cref="JavaMLWriter"/> instance for this ML instance.</returns>
        public JavaMLWriter Write() =>
            new JavaMLWriter((JvmObjectReference)Reference.Invoke("write"));

        /// <summary>
        /// Get the corresponding JavaMLReader instance.
        /// </summary>
        /// <returns>an <see cref="JavaMLReader&lt;Pipeline&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<Pipeline> Read() =>
            new JavaMLReader<Pipeline>((JvmObjectReference)Reference.Invoke("read"));

        private static Pipeline WrapAsPipeline(object obj) =>
            new Pipeline((JvmObjectReference)obj);
    }
}
