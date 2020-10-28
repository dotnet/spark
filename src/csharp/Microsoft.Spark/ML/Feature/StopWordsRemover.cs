// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// A <see cref="StopWordsRemover"/> feature transformer that filters out stop words from input.
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

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Sets the column that the <see cref="StopWordsRemover"/> should read from.
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetInputCol(string value) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setInputCol", value));

        /// <summary>
        /// The <see cref="StopWordsRemover"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the column to as the target</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetOutputCol(string value) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setOutputCol", value));

        /// <summary>
        /// Executes the <see cref="StopWordsRemover"/> and transforms the DataFrame to include the new
        /// column.
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns>
        /// New <see cref="DataFrame"/> object with the source <see cref="DataFrame"/> transformed
        /// </returns>
        public DataFrame Transform(DataFrame source) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));

        /// <summary>
        /// Gets the column that the <see cref="StopWordsRemover"/> should read from.
        /// </summary>
        /// <returns>string, input column</returns>
        public string GetInputCol() => (string)_jvmObject.Invoke("getInputCol");

        /// <summary>
        /// The <see cref="StopWordsRemover"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <returns>The output column name</returns>
        public string GetOutputCol() => (string)_jvmObject.Invoke("getOutputCol");

        /// <summary>
        /// Sets locale for <see cref="StopWordsRemover"/> transform.
        /// Refer java.util.locale.getavailablelocales() for all available locales.
        /// </summary>
        /// <param name="value">Locale to be used for transform</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetLocale(string value) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setLocale", value));

        /// <summary>
        /// Gets locale for <see cref="StopWordsRemover"/> transform
        /// </summary>
        /// <returns>The locale</returns>
        public string GetLocale() => (string)_jvmObject.Invoke("getLocale");

        /// <summary>
        /// Sets case sensitivity for <see cref="StopWordsRemover"/> transform.
        /// </summary>
        /// <param name="value">true if case sensitive, false otherwise</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetCaseSensitive(bool value) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setCaseSensitive", value));

        /// <summary>
        /// Gets case sensitivity.
        /// </summary>
        /// <returns>true if case sensitive, false otherwise</returns>
        public bool GetCaseSensitive() => (bool)_jvmObject.Invoke("getCaseSensitive");

        /// <summary>
        /// Sets custom stop words for <see cref="StopWordsRemover"/> transform.
        /// </summary>
        /// <param name="values">Custom stop words</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetStopWords(IEnumerable<string> values) =>
            WrapAsStopWordsRemover(_jvmObject.Invoke("setStopWords", values));

        /// <summary>
        /// Gets custom stop words for <see cref="StopWordsRemover"/> transform.
        /// </summary>
        /// <returns>Custom stop words</returns>
        public IEnumerable<string> GetStopWords() =>
            (IEnumerable<string>)_jvmObject.Invoke("getStopWords");

        /// <summary>
        /// Validate and get the transform schema for <see cref="StopWordsRemover"/> transform.
        /// </summary>
        /// <param name="value">Input schema</param>
        /// <returns>Output schema</returns>
        public StructType TransformSchema(StructType value) =>
            new StructType(
                (JvmObjectReference)_jvmObject.Invoke("transformSchema",
                    DataType.FromJson(_jvmObject.Jvm, value.Json)));

        /// <summary>
        /// Load default stop words of given language for <see cref="StopWordsRemover"/>
        /// transform Loads the default stop words for the given language.
        /// Supported languages: danish, dutch, english, finnish, french, german,
        /// hungarian, italian, norwegian, portuguese, russian, spanish, swedish, turkish.
        /// </summary>
        /// /// <param name="language">Language</param>
        /// <returns>Default stop words for the given language</returns>
        public static string[] LoadDefaultStopWords(string language) =>
            (string[])SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_stopWordsRemoverClassName, "loadDefaultStopWords", language);

        /// <summary>
        /// Loads the <see cref="StopWordsRemover"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">The path the previous <see cref="StopWordsRemover"/> was saved to</param>
        /// <returns>New <see cref="StopWordsRemover"/> object, loaded from path</returns>
        public static StopWordsRemover Load(string path) =>
            WrapAsStopWordsRemover(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(s_stopWordsRemoverClassName, "load", path));

        private static StopWordsRemover WrapAsStopWordsRemover(object obj) =>
            new StopWordsRemover((JvmObjectReference)obj);
    }
}
