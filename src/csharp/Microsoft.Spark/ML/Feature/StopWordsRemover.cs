// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// A <see cref="StopWordsRemover"/> that removes stop words from column.
    /// </summary>
    public class StopWordsRemover : FeatureBase<StopWordsRemover>, IJvmObjectReferenceProvider
    {
        private static readonly string s_stopWordsRemoverClassName =
            "org.apache.spark.ml.feature.StopWordsRemover";

        /// <summary>
        /// Create a <see cref="StopWordsRemover"/> without any parameters
        /// </summary>
        public StopWordsRemover() : base(s_stopWordsRemoverClassName)
        {
        }

        /// <summary>
        /// Create a <see cref="StopWordsRemover"/> with a UID that is used to give the
        /// <see cref="StopWordsRemover"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public StopWordsRemover(string uid) : base(s_stopWordsRemoverClassName, uid)
        {
        }

        internal StopWordsRemover(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets the column that the <see cref="StopWordsRemover"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetInputCol(string value) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setInputCol", value));

        /// <summary>
        /// Sets the column that the <see cref="StopWordsRemover"/> to save the result
        /// </summary>
        /// <param name="value">The name of the column to as the target</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetOutputCol(string value) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setOutputCol", value));

        /// <summary>
        /// Executes the <see cref="StopWordsRemover"/> and transforms the DataFrame to include the new
        /// column
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns>
        /// New <see cref="DataFrame"/> object with the source <see cref="DataFrame"/> transformed
        /// </returns>
        public DataFrame Transform(DataFrame source) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));

        /// <summary>
        /// Gets the column that the <see cref="StopWordsRemover"/> should read from
        /// </summary>
        /// <returns>string, input column</returns>
        public string GetInputCol() => (string)(_jvmObject.Invoke("getInputCol"));

        /// <summary>
        /// The <see cref="StopWordsRemover"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol() => (string)(_jvmObject.Invoke("getOutputCol"));

        /// <summary>
        /// Sets locale for <see cref="StopWordsRemover"/> transform
        /// </summary>
        /// <param name="value">Locale to to be used for transform</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetLocale(string value) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setLocale", value));

        /// <summary>
        /// Gets locale for <see cref="StopWordsRemover"/> transform
        /// </summary>
        /// <returns>string, the local</returns>
        public string GetLocale() => (string)(_jvmObject.Invoke("getLocale"));

        /// <summary>
        /// Loads the <see cref="StopWordsRemover"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">The path the previous <see cref="StopWordsRemover"/> was saved to</param>
        /// <returns>New <see cref="StopWordsRemover"/> object, loaded from path</returns>
        public static StopWordsRemover Load(string path)
        {
            return WrapAsStopWordsRemover(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_stopWordsRemoverClassName, "load", path));
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        private static StopWordsRemover WrapAsStopWordsRemover(object obj) =>
            new StopWordsRemover((JvmObjectReference)obj);
    }
}
