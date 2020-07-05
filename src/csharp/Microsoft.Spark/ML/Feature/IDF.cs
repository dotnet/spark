// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// Inverse document frequency (IDF). The standard formulation is used:
    /// idf = log((m + 1) / (d(t) + 1)), where m is the total number of documents and d(t) is
    /// the number of documents that contain term t.
    /// 
    /// This implementation supports filtering out terms which do not appear in a minimum number
    /// of documents (controlled by the variable minDocFreq). For terms that are not in at least
    /// minDocFreq documents, the IDF is found as 0, resulting in TF-IDFs of 0.
    /// </summary>
    public class IDF : FeatureBase<IDF>, IJvmObjectReferenceProvider
    {
        private static readonly string s_IDFClassName = "org.apache.spark.ml.feature.IDF";
        
        /// <summary>
        /// Create a <see cref="IDF"/> without any parameters
        /// </summary>
        public IDF() : base(s_IDFClassName)
        {
        }

        /// <summary>
        /// Create a <see cref="IDF"/> with a UID that is used to give the
        /// <see cref="IDF"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public IDF(string uid) : base(s_IDFClassName, uid)
        {
        }
        
        internal IDF(JvmObjectReference jvmObject) : base(jvmObject, s_IDFClassName)
        {
        }
        
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Gets the column that the <see cref="IDF"/> should read from
        /// </summary>
        /// <returns>string, input column</returns>
        public string GetInputCol() => (string)(_jvmObject.Invoke("getInputCol"));

        /// <summary>
        /// Sets the column that the <see cref="IDF"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="IDF"/> object</returns>
        public IDF SetInputCol(string value) => WrapAsIDF(_jvmObject.Invoke("setInputCol", value));

        /// <summary>
        /// The <see cref="IDF"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol() => (string)(_jvmObject.Invoke("getOutputCol"));

        /// <summary>
        /// The <see cref="IDF"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column</param>
        /// <returns>New <see cref="IDF"/> object</returns>
        public IDF SetOutputCol(string value) => 
            WrapAsIDF(_jvmObject.Invoke("setOutputCol", value));

        /// <summary>
        /// Minimum of documents in which a term should appear for filtering
        /// </summary>
        /// <returns>int, minimum number of documents in which a term should appear</returns>
        public int GetMinDocFreq() => (int)_jvmObject.Invoke("getMinDocFreq");

        /// <summary>
        /// Minimum of documents in which a term should appear for filtering
        /// </summary>
        /// <param name="value">int, the minimum of documents a term should appear in</param>
        /// <returns>New <see cref="IDF"/> object</returns>
        public IDF SetMinDocFreq(int value) => 
            WrapAsIDF(_jvmObject.Invoke("setMinDocFreq", value));

        /// <summary>
        /// Fits a model to the input data.
        /// </summary>
        /// <param name="source">The <see cref="DataFrame"/> to fit the model to</param>
        /// <returns>New <see cref="IDFModel"/> object</returns>
        public IDFModel Fit(DataFrame source) => 
            new IDFModel((JvmObjectReference)_jvmObject.Invoke("fit", source));

        /// <summary>
        /// Loads the <see cref="IDF"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">The path the previous <see cref="IDF"/> was saved to</param>
        /// <returns>New <see cref="IDF"/> object, loaded from path</returns>
        public static IDF Load(string path)
        {
            return WrapAsIDF(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(s_IDFClassName, "load", path));
        }
 
        private static IDF WrapAsIDF(object obj) => new IDF((JvmObjectReference)obj);
    }
}
