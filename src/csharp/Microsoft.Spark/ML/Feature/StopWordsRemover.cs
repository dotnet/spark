// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.ML.Feature
{
    class StopWordsRemover : FeatureBase<StopWordsRemover>
    {
        private static readonly string s_stopWordsRemoverClassName =
           "org.apache.spark.ml.feature.StopWordsRemover";

        /// <summary>
        /// Creates a <see cref="StopWordsRemover"/> without any parameters.
        /// </summary>
        public StopWordsRemover() : base(s_stopWordsRemoverClassName)
        {
        }

        internal StopWordsRemover(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Creates a <see cref="StopWordsRemover"/> with a UID that is used to give the
        /// <see cref="StopWordsRemover"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public StopWordsRemover(string uid) : base(s_stopWordsRemoverClassName, uid)
        {
        }

        public bool GetCaseSensitive() => (bool)_jvmObject.Invoke("getCaseSensitive");

        public StopWordsRemover SetCaseSensitive(bool value) => 
            WrapAsStopWordsRemover(_jvmObject.Invoke("setCaseSensitive", value));

        public string GetLocale() => (string)_jvmObject.Invoke("getLocale");

        public StopWordsRemover SetLocale(string value) => 
            WrapAsStopWordsRemover(_jvmObject.Invoke("setLocale", value));

        public string[] GetStopWords() => (string[])_jvmObject.Invoke("getStopWords");

        public StopWordsRemover SetStopWords(string[] value) => 
            WrapAsStopWordsRemover(_jvmObject.Invoke("setStopWords", value));

        public string GetInputCol() => (string)_jvmObject.Invoke("getInputCol");

        public StopWordsRemover SetInputCol(string value) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setInputCol", value));

        public string GetOutputCol() => (string)_jvmObject.Invoke("getOutputCol");

        public StopWordsRemover SetOutputCol(string value) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setOutputCol", value));

        /// <summary>
        /// Check transform validity and derive the output schema from the input schema.
        /// 
        /// This checks for validity of interactions between parameters during Transform and
        /// raises an exception if any parameter value is invalid.
        ///
        /// Typical implementation should first conduct verification on schema change and parameter
        /// validity, including complex parameter interaction checks.
        /// </summary>
        /// <param name="value">
        /// The <see cref="StructType"/> of the <see cref="DataFrame"/> which will be transformed.
        /// </param>
        /// <returns>
        /// The <see cref="StructType"/> of the output schema that would have been derived from the
        /// input schema, if Transform had been called.
        /// </returns>
        public StructType TransformSchema(StructType value) =>
            new StructType(
                (JvmObjectReference)_jvmObject.Invoke(
                    "transformSchema",
                    DataType.FromJson(_jvmObject.Jvm, value.Json)));

        /// <summary>
        /// Removes the stop words from a document in a DataFrame.
        /// </summary>
        /// <param name="document"><see cref="DataFrame"/> to transform</param>
        /// <returns><see cref="DataFrame"/> containing the stop words to remove</returns>
        public DataFrame Transform(DataFrame document) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", document));

        public static string[] LoadDefaultStopWords(string language) => 
            (string[])SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_stopWordsRemoverClassName, "loadDefaultStopWords", language);

        /// <summary>
        /// Loads the <see cref="StopWordsRemover"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">
        /// The path the previous <see cref="StopWordsRemover"/> was saved to.
        /// </param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public static StopWordsRemover Load(string path) =>
            WrapAsStopWordsRemover((JvmObjectReference)
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_stopWordsRemoverClassName, "load", path));

        private static StopWordsRemover WrapAsStopWordsRemover(object obj) =>
            new StopWordsRemover((JvmObjectReference)obj);

    }
}
