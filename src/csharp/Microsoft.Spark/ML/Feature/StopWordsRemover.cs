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
    public class StopWordsRemover :
        JavaTransformer,
        IJavaMLWritable,
        IJavaMLReadable<StopWordsRemover>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.StopWordsRemover";

        /// <summary>
        /// Create a <see cref="StopWordsRemover"/> without any parameters.
        /// </summary>
        public StopWordsRemover() : base(s_className)
        {
        }

        /// <summary>
        /// Create a <see cref="StopWordsRemover"/> with a UID that is used to give the
        /// <see cref="StopWordsRemover"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public StopWordsRemover(string uid) : base(s_className, uid)
        {
        }

        internal StopWordsRemover(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets the column that the <see cref="StopWordsRemover"/> should read from.
        /// </summary>
        /// <param name="value">The name of the column to use as the source</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetInputCol(string value) =>
            WrapAsStopWordsRemover(Reference.Invoke("setInputCol", value));

        /// <summary>
        /// The <see cref="StopWordsRemover"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the column to use as the target</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetOutputCol(string value) =>
            WrapAsStopWordsRemover(Reference.Invoke("setOutputCol", value));

        /// <summary>
        /// Executes the <see cref="StopWordsRemover"/> and transforms the DataFrame to include the new
        /// column.
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns>
        /// New <see cref="DataFrame"/> object with the source <see cref="DataFrame"/> transformed
        /// </returns>
        public override DataFrame Transform(DataFrame source) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", source));

        /// <summary>
        /// Gets the column that the <see cref="StopWordsRemover"/> should read from.
        /// </summary>
        /// <returns>Input column name</returns>
        public string GetInputCol() => (string)Reference.Invoke("getInputCol");

        /// <summary>
        /// The <see cref="StopWordsRemover"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <returns>The output column name</returns>
        public string GetOutputCol() => (string)Reference.Invoke("getOutputCol");

        /// <summary>
        /// Sets locale for <see cref="StopWordsRemover"/> transform.
        /// Refer java.util.locale.getavailablelocales() for all available locales.
        /// </summary>
        /// <param name="value">Locale to be used for transform</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        [Since(Versions.V2_4_0)]
        public StopWordsRemover SetLocale(string value) =>
            WrapAsStopWordsRemover(Reference.Invoke("setLocale", value));

        /// <summary>
        /// Gets locale for <see cref="StopWordsRemover"/> transform
        /// </summary>
        /// <returns>The locale</returns>
        [Since(Versions.V2_4_0)]
        public string GetLocale() => (string)Reference.Invoke("getLocale");

        /// <summary>
        /// Sets case sensitivity.
        /// </summary>
        /// <param name="value">true if case sensitive, false otherwise</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetCaseSensitive(bool value) =>
            WrapAsStopWordsRemover(Reference.Invoke("setCaseSensitive", value));

        /// <summary>
        /// Gets case sensitivity.
        /// </summary>
        /// <returns>true if case sensitive, false otherwise</returns>
        public bool GetCaseSensitive() => (bool)Reference.Invoke("getCaseSensitive");

        /// <summary>
        /// Sets custom stop words.
        /// </summary>
        /// <param name="values">Custom stop words</param>
        /// <returns>New <see cref="StopWordsRemover"/> object</returns>
        public StopWordsRemover SetStopWords(IEnumerable<string> values) =>
            WrapAsStopWordsRemover(Reference.Invoke("setStopWords", values));

        /// <summary>
        /// Gets the custom stop words.
        /// </summary>
        /// <returns>Custom stop words</returns>
        public IEnumerable<string> GetStopWords() =>
            (IEnumerable<string>)Reference.Invoke("getStopWords");

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
        public override StructType TransformSchema(StructType value) =>
            new StructType(
                (JvmObjectReference)Reference.Invoke(
                    "transformSchema",
                    DataType.FromJson(Reference.Jvm, value.Json)));

        /// <summary>
        /// Load default stop words of given language for <see cref="StopWordsRemover"/>
        /// transform.
        /// Supported languages: danish, dutch, english, finnish, french, german,
        /// hungarian, italian, norwegian, portuguese, russian, spanish, swedish, turkish.
        /// </summary>
        /// <param name="language">Language</param>
        /// <returns>Default stop words for the given language</returns>
        public static string[] LoadDefaultStopWords(string language) =>
            (string[])SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_className, "loadDefaultStopWords", language);

        /// <summary>
        /// Loads the <see cref="StopWordsRemover"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">The path the previous <see cref="StopWordsRemover"/> was saved to</param>
        /// <returns>New <see cref="StopWordsRemover"/> object, loaded from path</returns>
        public static StopWordsRemover Load(string path) =>
            WrapAsStopWordsRemover(
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
        /// <returns>an <see cref="JavaMLReader&lt;StopWordsRemover&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<StopWordsRemover> Read() =>
            new JavaMLReader<StopWordsRemover>((JvmObjectReference)Reference.Invoke("read"));

        private static StopWordsRemover WrapAsStopWordsRemover(object obj) =>
            new StopWordsRemover((JvmObjectReference)obj);
    }
}
