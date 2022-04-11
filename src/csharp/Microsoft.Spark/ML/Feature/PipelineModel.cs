// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// <see cref="PipelineModel"/> Represents a fitted pipeline.
    /// </summary>
    public class PipelineModel : ScalaModel<PipelineModel>, IScalaMLWritable, IScalaMLReadable<PipelineModel>
    {
        private static readonly string s_pipelineModelClassName = "org.apache.spark.ml.PipelineModel";

        /// <summary>
        /// Creates a <see cref="PipelineModel"/> with a UID that is used to give the
        /// <see cref="PipelineModel"/> a unique ID, and an array of transformers as stages.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        /// <param name="stages">Stages for the PipelineModel.</param>
        public PipelineModel(string uid, ScalaTransformer[] stages)
            : this(SparkEnvironment.JvmBridge.CallConstructor(
                s_pipelineModelClassName, uid, stages.ToJavaArrayList()))
        {
        }

        internal PipelineModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Loads the <see cref="PipelineModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="PipelineModel"/> was saved to</param>
        /// <returns>New <see cref="PipelineModel"/> object, loaded from path.</returns>
        public static PipelineModel Load(string path) => WrapAsPipelineModel(
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(s_pipelineModelClassName, "load", path));

        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        public void Save(string path) => Reference.Invoke("save", path);

        /// <summary>
        /// Get the corresponding ScalaMLWriter instance.
        /// </summary>
        /// <returns>a <see cref="ScalaMLWriter"/> instance for this ML instance.</returns>
        public ScalaMLWriter Write() =>
            new ScalaMLWriter((JvmObjectReference)Reference.Invoke("write"));

        /// <summary>
        /// Get the corresponding ScalaMLReader instance.
        /// </summary>
        /// <returns>an <see cref="ScalaMLReader&lt;PipelineModel&gt;"/> instance for this ML instance.</returns>
        public ScalaMLReader<PipelineModel> Read() =>
            new ScalaMLReader<PipelineModel>((JvmObjectReference)Reference.Invoke("read"));

        private static PipelineModel WrapAsPipelineModel(object obj) =>
            new PipelineModel((JvmObjectReference)obj);

    }
}
