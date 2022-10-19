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
    public class CountVectorizerModel :
        JavaModel<CountVectorizerModel>,
        IJavaMLWritable,
        IJavaMLReadable<CountVectorizerModel>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.CountVectorizerModel";

        /// <summary>
        /// Creates a <see cref="CountVectorizerModel"/> without any parameters
        /// </summary>
        /// <param name="vocabulary">The vocabulary to use</param>
        public CountVectorizerModel(List<string> vocabulary)
            : this(SparkEnvironment.JvmBridge.CallConstructor(
                s_className, vocabulary))
        {
        }

        /// <summary>
        /// Creates a <see cref="CountVectorizerModel"/> with a UID that is used to give the
        /// <see cref="CountVectorizerModel"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        /// <param name="vocabulary">The vocabulary to use</param>
        public CountVectorizerModel(string uid, List<string> vocabulary)
            : this(SparkEnvironment.JvmBridge.CallConstructor(
                s_className, uid, vocabulary))
        {
        }

        internal CountVectorizerModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Loads the <see cref="CountVectorizerModel"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">
        /// The path the previous <see cref="CountVectorizerModel"/> was saved to
        /// </param>
        /// <returns>New <see cref="CountVectorizerModel"/> object</returns>
        public static CountVectorizerModel Load(string path) =>
            WrapAsCountVectorizerModel(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_className, "load", path));

        /// <summary>
        /// Gets the binary toggle to control the output vector values. If True, all nonzero counts
        /// (after minTF filter applied) are set to 1. This is useful for discrete probabilistic
        /// models that model binary events rather than integer counts. Default: false
        /// </summary>
        /// <returns>Toggle value of type boolean</returns>
        public bool GetBinary() => (bool)Reference.Invoke("getBinary");

        /// <summary>
        /// Sets the binary toggle to control the output vector values. If True, all nonzero counts
        /// (after minTF filter applied) are set to 1. This is useful for discrete probabilistic
        /// models that model binary events rather than integer counts. Default: false
        /// </summary>
        /// <param name="value">Turn the binary toggle on or off</param>
        /// <returns>
        /// <see cref="CountVectorizerModel"/> with the new binary toggle value set
        /// </returns>
        public CountVectorizerModel SetBinary(bool value) =>
            WrapAsCountVectorizerModel(Reference.Invoke("setBinary", value));

        /// <summary>
        /// Gets the column that the <see cref="CountVectorizerModel"/> should read from and
        /// convert into buckets. This would have been set by SetInputCol
        /// </summary>
        /// <returns>string, the input column</returns>
        public string GetInputCol() => (string)Reference.Invoke("getInputCol");

        /// <summary>
        /// Sets the column that the <see cref="CountVectorizerModel"/> should read from.
        /// </summary>
        /// <param name="value">The name of the column to use as the source.</param>
        /// <returns><see cref="CountVectorizerModel"/> with the input column set</returns>
        public CountVectorizerModel SetInputCol(string value) =>
            WrapAsCountVectorizerModel(Reference.Invoke("setInputCol", value));

        /// <summary>
        /// Gets the name of the new column the <see cref="CountVectorizerModel"/> will create in
        /// the DataFrame.
        /// </summary>
        /// <returns>The name of the output column.</returns>
        public string GetOutputCol() => (string)Reference.Invoke("getOutputCol");

        /// <summary>
        /// Sets the name of the new column the <see cref="CountVectorizerModel"/> will create in
        /// the DataFrame.
        /// </summary>
        /// <param name="value">The name of the output column which will be created.</param>
        /// <returns>New <see cref="CountVectorizerModel"/> with the output column set</returns>
        public CountVectorizerModel SetOutputCol(string value) =>
            WrapAsCountVectorizerModel(Reference.Invoke("setOutputCol", value));

        /// <summary>
        /// Gets the maximum number of different documents a term could appear in to be included in
        /// the vocabulary. A term that appears more than the threshold will be ignored. If this is
        /// an integer greater than or equal to 1, this specifies the maximum number of documents
        /// the term could appear in; if this is a double in [0,1), then this specifies the maximum
        /// fraction of documents the term could appear in.
        /// </summary>
        /// <returns>The maximum document term frequency of type double.</returns>
        public double GetMaxDF() => (double)Reference.Invoke("getMaxDF");

        /// <summary>
        /// Gets the minimum number of different documents a term must appear in to be included in
        /// the vocabulary. If this is an integer greater than or equal to 1, this specifies the
        /// number of documents the term must appear in; if this is a double in [0,1), then this
        /// specifies the fraction of documents.
        /// </summary>
        /// <returns>The minimum document term frequency</returns>
        public double GetMinDF() => (double)Reference.Invoke("getMinDF");

        /// <summary>
        /// Gets the filter to ignore rare words in a document. For each document, terms with
        /// frequency/count less than the given threshold are ignored. If this is an integer
        /// greater than or equal to 1, then this specifies a count (of times the term must appear
        /// in the document); if this is a double in [0,1), then this specifies a fraction (out of
        /// the document's token count).
        ///
        /// Note that the parameter is only used in transform of CountVectorizerModel and does not
        /// affect fitting.
        /// </summary>
        /// <returns>Minimum term frequency of type double.</returns>
        public double GetMinTF() => (double)Reference.Invoke("getMinTF");

        /// <summary>
        /// Sets the filter to ignore rare words in a document. For each document, terms with
        /// frequency/count less than the given threshold are ignored. If this is an integer
        /// greater than or equal to 1, then this specifies a count (of times the term must appear
        /// in the document); if this is a double in [0,1), then this specifies a fraction (out of
        /// the document's token count).
        ///
        /// Note that the parameter is only used in transform of CountVectorizerModel and does not
        /// affect fitting.
        /// </summary>
        /// <param name="value">Minimum term frequency of type double.</param>
        /// <returns>
        /// New <see cref="CountVectorizerModel"/> with the min term frequency set
        /// </returns>
        public CountVectorizerModel SetMinTF(double value) =>
            WrapAsCountVectorizerModel(Reference.Invoke("setMinTF", value));

        /// <summary>
        /// Gets the max size of the vocabulary. <see cref="CountVectorizerModel"/> will build a
        /// vocabulary that only considers the top vocabSize terms ordered by term frequency across
        /// the corpus.
        /// </summary>
        /// <returns>The max size of the vocabulary of type int.</returns>
        public int GetVocabSize() => (int)Reference.Invoke("getVocabSize");

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
        /// Converts a DataFrame with a text document to a sparse vector of token counts.
        /// </summary>
        /// <param name="document"><see cref="DataFrame"/> to transform</param>
        /// <returns><see cref="DataFrame"/> containing the original data and the counts</returns>
        public override DataFrame Transform(DataFrame document) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", document));

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
        /// <returns>an <see cref="JavaMLReader&lt;CountVectorizerModel&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<CountVectorizerModel> Read() =>
            new JavaMLReader<CountVectorizerModel>((JvmObjectReference)Reference.Invoke("read"));

        private static CountVectorizerModel WrapAsCountVectorizerModel(object obj) =>
            new CountVectorizerModel((JvmObjectReference)obj);
    }
}
