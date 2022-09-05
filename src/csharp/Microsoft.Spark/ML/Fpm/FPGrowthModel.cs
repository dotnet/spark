// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Fpm
{
    /// <summary>
    /// <see cref="FPGrowthModel"/> implements FPGrowthModel
    /// </summary>
    public class FPGrowthModel : JavaModel<FPGrowthModel>, IJavaMLWritable, IJavaMLReadable<FPGrowthModel>
    {
        private static readonly string s_className = "org.apache.spark.ml.fpm.FPGrowthModel";

        /// <summary>
        /// Creates a <see cref="FPGrowthModel"/> without any parameters.
        /// </summary>
        public FPGrowthModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="FPGrowthModel"/> with a UID that is used to give the
        /// <see cref="FPGrowthModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public FPGrowthModel(string uid) : base(s_className, uid)
        {
        }

        internal FPGrowthModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets value for itemsCol
        /// </summary>
        /// <param name="value">
        /// items column name
        /// </param>
        /// <returns> New FPGrowthModel object </returns>
        public FPGrowthModel SetItemsCol(string value) =>
            WrapAsFPGrowthModel(Reference.Invoke("setItemsCol", (object)value));
        
        /// <summary>
        /// Sets value for minConfidence
        /// </summary>
        /// <param name="value">
        /// minimal confidence for generating Association Rule
        /// </param>
        /// <returns> New FPGrowthModel object </returns>
        public FPGrowthModel SetMinConfidence(double value) =>
            WrapAsFPGrowthModel(Reference.Invoke("setMinConfidence", (object)value));
        
        /// <summary>
        /// Sets value for minSupport
        /// </summary>
        /// <param name="value">
        /// the minimal support level of a frequent pattern
        /// </param>
        /// <returns> New FPGrowthModel object </returns>
        public FPGrowthModel SetMinSupport(double value) =>
            WrapAsFPGrowthModel(Reference.Invoke("setMinSupport", (object)value));
        
        /// <summary>
        /// Sets value for numPartitions
        /// </summary>
        /// <param name="value">
        /// Number of partitions used by parallel FP-growth
        /// </param>
        /// <returns> New FPGrowthModel object </returns>
        public FPGrowthModel SetNumPartitions(int value) =>
            WrapAsFPGrowthModel(Reference.Invoke("setNumPartitions", (object)value));
        
        /// <summary>
        /// Sets value for predictionCol
        /// </summary>
        /// <param name="value">
        /// prediction column name
        /// </param>
        /// <returns> New FPGrowthModel object </returns>
        public FPGrowthModel SetPredictionCol(string value) =>
            WrapAsFPGrowthModel(Reference.Invoke("setPredictionCol", (object)value));
        /// <summary>
        /// Gets itemsCol value
        /// </summary>
        /// <returns>
        /// itemsCol: items column name
        /// </returns>
        public string GetItemsCol() =>
            (string)Reference.Invoke("getItemsCol");
        
        /// <summary>
        /// Gets minConfidence value
        /// </summary>
        /// <returns>
        /// minConfidence: minimal confidence for generating Association Rule
        /// </returns>
        public double GetMinConfidence() =>
            (double)Reference.Invoke("getMinConfidence");
        
        /// <summary>
        /// Gets minSupport value
        /// </summary>
        /// <returns>
        /// minSupport: the minimal support level of a frequent pattern
        /// </returns>
        public double GetMinSupport() =>
            (double)Reference.Invoke("getMinSupport");
        
        /// <summary>
        /// Gets numPartitions value
        /// </summary>
        /// <returns>
        /// numPartitions: Number of partitions used by parallel FP-growth
        /// </returns>
        public int GetNumPartitions() =>
            (int)Reference.Invoke("getNumPartitions");
        
        /// <summary>
        /// Gets predictionCol value
        /// </summary>
        /// <returns>
        /// predictionCol: prediction column name
        /// </returns>
        public string GetPredictionCol() =>
            (string)Reference.Invoke("getPredictionCol");
        
        /// <summary>
        /// Loads the <see cref="FPGrowthModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="FPGrowthModel"/> was saved to</param>
        /// <returns>New <see cref="FPGrowthModel"/> object, loaded from path.</returns>
        public static FPGrowthModel Load(string path) => WrapAsFPGrowthModel(
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(s_className, "load", path));
        
        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        public void Save(string path) => Reference.Invoke("save", path);
        
        /// <returns>a <see cref="JavaMLWriter"/> instance for this ML instance.</returns>
        public JavaMLWriter Write() =>
            new JavaMLWriter((JvmObjectReference)Reference.Invoke("write"));
        
        /// <summary>
        /// Get the corresponding JavaMLReader instance.
        /// </summary>
        /// <returns>an <see cref="JavaMLReader&lt;FPGrowthModel&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<FPGrowthModel> Read() =>
            new JavaMLReader<FPGrowthModel>((JvmObjectReference)Reference.Invoke("read"));
        private static FPGrowthModel WrapAsFPGrowthModel(object obj) =>
            new FPGrowthModel((JvmObjectReference)obj);
        
    }
}

        