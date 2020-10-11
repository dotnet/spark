// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// Class <see cref="NGram"/> transformer that converts the input array of strings into an array of n-grams.
    /// Null values in the input array are ignored.
    /// It returns an array of n-grams where each n-gram is represented by a space-separated string of words.
    /// </summary>
    public class NGram : FeatureBase<NGram>, IJvmObjectReferenceProvider
    {
        private static readonly string s_NGramClassName =
            "org.apache.spark.ml.feature.NGram";

        /// <summary>
        /// Create a <see cref="NGram"/> without any parameters
        /// </summary>
        public NGram() : base(s_NGramClassName)
        {
        }

        /// <summary>
        /// Create a <see cref="NGram"/> with a UID that is used to give the
        /// <see cref="NGram"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public NGram(string uid) : base(s_NGramClassName, uid)
        {
        }

        internal NGram(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Gets the column that the <see cref="NGram"/> should read from
        /// </summary>
        /// <returns>string, input column</returns>
        public string GetInputCol() => (string)(_jvmObject.Invoke("getInputCol"));

        /// <summary>
        /// Sets the column that the <see cref="NGram"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="NGram"/> object</returns>
        public NGram SetInputCol(string value) =>
            WrapAsNGram(_jvmObject.Invoke("setInputCol", value));

        /// <summary>
        /// The <see cref="NGram"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol() => (string)(_jvmObject.Invoke("getOutputCol"));

        /// <summary>
        /// The <see cref="NGram"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column</param>
        /// <returns>New <see cref="NGram"/> object</returns>
        public NGram SetOutputCol(string value) =>
            WrapAsNGram(_jvmObject.Invoke("setOutputCol", value));

        /// <summary>
        /// Gets N value for <see cref="NGram"/>
        /// </summary>
        /// <returns>string, N value</returns>
        public int GetN() => (int)(_jvmObject.Invoke("getN"));

        /// <summary>
        /// Sets N value for <see cref="NGram"/>
        /// </summary>
        /// <param name="value">N value</param>
        /// <returns>New <see cref="NGram"/> object</returns>
        public NGram SetN(int value) =>
            WrapAsNGram(_jvmObject.Invoke("setN", value));

        /// <summary>
        /// Executes the <see cref="NGram"/> and transforms the DataFrame to include the new
        /// column
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns>
        /// New <see cref="DataFrame"/> object with the source <see cref="DataFrame"/> transformed
        /// </returns>
        public DataFrame Transform(DataFrame source) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));

        /// <summary>
        /// Loads the <see cref="NGram"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">The path the previous <see cref="NGram"/> was saved to</param>
        /// <returns>New <see cref="NGram"/> object, loaded from path</returns>
        public static NGram Load(string path)
        {
            return WrapAsNGram(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_NGramClassName, "load", path));
        }


        private static NGram WrapAsNGram(object obj) =>
            new NGram((JvmObjectReference)obj);
    }
}
