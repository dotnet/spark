// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Fpm
{
    /// <summary>
    /// <see cref="FPGrowth"/> implements FPGrowth
    /// </summary>
    public class FPGrowth : JavaEstimator<FPGrowthModel>, IJavaMLWritable, IJavaMLReadable<FPGrowth>
    {

        private static readonly string s_className = "org.apache.spark.ml.fpm.FPGrowth";

        /// <summary>
        /// Creates a <see cref="FPGrowth"/> without any parameters.
        /// </summary>
        public FPGrowth() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="FPGrowth"/> with a UID that is used to give the
        /// <see cref="FPGrowth"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public FPGrowth(string uid) : base(s_className, uid)
        {
        }

        internal FPGrowth(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets value for itemsCol
        /// </summary>
        /// <param name="value">
        /// items column name
        /// </param>
        /// <returns> New FPGrowth object </returns>
        public FPGrowth SetItemsCol(string value) =>
            WrapAsFPGrowth(Reference.Invoke("setItemsCol", (object)value));
        
        /// <summary>
        /// Sets value for minConfidence
        /// </summary>
        /// <param name="value">
        /// minimal confidence for generating Association Rule
        /// </param>
        /// <returns> New FPGrowth object </returns>
        public FPGrowth SetMinConfidence(double value) =>
            WrapAsFPGrowth(Reference.Invoke("setMinConfidence", (object)value));
        
        /// <summary>
        /// Sets value for minSupport
        /// </summary>
        /// <param name="value">
        /// the minimal support level of a frequent pattern
        /// </param>
        /// <returns> New FPGrowth object </returns>
        public FPGrowth SetMinSupport(double value) =>
            WrapAsFPGrowth(Reference.Invoke("setMinSupport", (object)value));
        
        /// <summary>
        /// Sets value for numPartitions
        /// </summary>
        /// <param name="value">
        /// Number of partitions used by parallel FP-growth
        /// </param>
        /// <returns> New FPGrowth object </returns>
        public FPGrowth SetNumPartitions(int value) =>
            WrapAsFPGrowth(Reference.Invoke("setNumPartitions", (object)value));
        
        /// <summary>
        /// Sets value for predictionCol
        /// </summary>
        /// <param name="value">
        /// prediction column name
        /// </param>
        /// <returns> New FPGrowth object </returns>
        public FPGrowth SetPredictionCol(string value) =>
            WrapAsFPGrowth(Reference.Invoke("setPredictionCol", (object)value));
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
        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="FPGrowthModel"/></returns>
        override public FPGrowthModel Fit(DataFrame dataset) =>
            new FPGrowthModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));
        /// <summary>
        /// Loads the <see cref="FPGrowth"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="FPGrowth"/> was saved to</param>
        /// <returns>New <see cref="FPGrowth"/> object, loaded from path.</returns>
        public static FPGrowth Load(string path) => WrapAsFPGrowth(
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
        /// <returns>an <see cref="JavaMLReader&lt;FPGrowth&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<FPGrowth> Read() =>
            new JavaMLReader<FPGrowth>((JvmObjectReference)Reference.Invoke("read"));
        private static FPGrowth WrapAsFPGrowth(object obj) =>
            new FPGrowth((JvmObjectReference)obj);
        
    }
}

        