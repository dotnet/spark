// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    public class CountVectorizer :
        JavaEstimator<CountVectorizerModel>,
        IJavaMLWritable,
        IJavaMLReadable<CountVectorizer>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.CountVectorizer";

        /// <summary>
        /// Creates a <see cref="CountVectorizer"/> without any parameters.
        /// </summary>
        public CountVectorizer() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="CountVectorizer"/> with a UID that is used to give the
        /// <see cref="CountVectorizer"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public CountVectorizer(string uid) : base(s_className, uid)
        {
        }

        internal CountVectorizer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataFrame">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="CountVectorizerModel"/></returns>
        public override CountVectorizerModel Fit(DataFrame dataFrame) =>
            new CountVectorizerModel((JvmObjectReference)Reference.Invoke("fit", dataFrame));

        /// <summary>
        /// Loads the <see cref="CountVectorizer"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">
        /// The path the previous <see cref="CountVectorizer"/> was saved to.
        /// </param>
        /// <returns>New <see cref="CountVectorizer"/> object</returns>
        public static CountVectorizer Load(string path) =>
            WrapAsCountVectorizer((JvmObjectReference)
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_className, "load", path));

        /// <summary>
        /// Gets the binary toggle to control the output vector values. If True, all nonzero counts
        /// (after minTF filter applied) are set to 1. This is useful for discrete probabilistic
        /// models that model binary events rather than integer counts. Default: false
        /// </summary>
        /// <returns>boolean</returns>
        public bool GetBinary() => (bool)Reference.Invoke("getBinary");

        /// <summary>
        /// Sets the binary toggle to control the output vector values. If True, all nonzero counts
        /// (after minTF filter applied) are set to 1. This is useful for discrete probabilistic
        /// models that model binary events rather than integer counts. Default: false
        /// </summary>
        /// <param name="value">Turn the binary toggle on or off</param>
        /// <returns><see cref="CountVectorizer"/> with the new binary toggle value set</returns>
        public CountVectorizer SetBinary(bool value) =>
            WrapAsCountVectorizer((JvmObjectReference)Reference.Invoke("setBinary", value));

        /// <summary>
        /// Gets the column that the <see cref="CountVectorizer"/> should read from and convert
        /// into buckets. This would have been set by SetInputCol.
        /// </summary>
        /// <returns>The input column of type string</returns>
        public string GetInputCol() => (string)Reference.Invoke("getInputCol");

        /// <summary>
        /// Sets the column that the <see cref="CountVectorizer"/> should read from.
        /// </summary>
        /// <param name="value">The name of the column to use as the source.</param>
        /// <returns><see cref="CountVectorizer"/> with the input column set</returns>
        public CountVectorizer SetInputCol(string value) =>
            WrapAsCountVectorizer((JvmObjectReference)Reference.Invoke("setInputCol", value));

        /// <summary>
        /// Gets the name of the new column the <see cref="CountVectorizer"/> creates in the
        /// DataFrame.
        /// </summary>
        /// <returns>The name of the output column.</returns>
        public string GetOutputCol() => (string)Reference.Invoke("getOutputCol");

        /// <summary>
        /// Sets the name of the new column the <see cref="CountVectorizer"/> creates in the
        /// DataFrame.
        /// </summary>
        /// <param name="value">The name of the output column which will be created.</param>
        /// <returns>New <see cref="CountVectorizer"/> with the output column set</returns>
        public CountVectorizer SetOutputCol(string value) =>
            WrapAsCountVectorizer((JvmObjectReference)Reference.Invoke("setOutputCol", value));

        /// <summary>
        /// Gets the maximum number of different documents a term could appear in to be included in
        /// the vocabulary. A term that appears more than the threshold will be ignored. If this is
        /// an integer greater than or equal to 1, this specifies the maximum number of documents
        /// the term could appear in; if this is a double in [0,1), then this specifies the maximum
        /// fraction of documents the term could appear in.
        /// </summary>
        /// <returns>The maximum document term frequency</returns>
        [Since(Versions.V2_4_0)]
        public double GetMaxDF() => (double)Reference.Invoke("getMaxDF");

        /// <summary>
        /// Sets the maximum number of different documents a term could appear in to be included in
        /// the vocabulary. A term that appears more than the threshold will be ignored. If this is
        /// an integer greater than or equal to 1, this specifies the maximum number of documents
        /// the term could appear in; if this is a double in [0,1), then this specifies the maximum
        /// fraction of documents the term could appear in.
        /// </summary>
        /// <param name="value">The maximum document term frequency</param>
        /// <returns>New <see cref="CountVectorizer"/> with the max df value set</returns>
        [Since(Versions.V2_4_0)]
        public CountVectorizer SetMaxDF(double value) =>
            WrapAsCountVectorizer((JvmObjectReference)Reference.Invoke("setMaxDF", value));

        /// <summary>
        /// Gets the minimum number of different documents a term must appear in to be included in
        /// the vocabulary. If this is an integer greater than or equal to 1, this specifies the
        /// number of documents the term must appear in; if this is a double in [0,1), then this
        /// specifies the fraction of documents.
        /// </summary>
        /// <returns>The minimum document term frequency</returns>
        public double GetMinDF() => (double)Reference.Invoke("getMinDF");

        /// <summary>
        /// Sets the minimum number of different documents a term must appear in to be included in
        /// the vocabulary. If this is an integer greater than or equal to 1, this specifies the
        /// number of documents the term must appear in; if this is a double in [0,1), then this
        /// specifies the fraction of documents.
        /// </summary>
        /// <param name="value">The minimum document term frequency</param>
        /// <returns>New <see cref="CountVectorizer"/> with the min df value set</returns>
        public CountVectorizer SetMinDF(double value) =>
            WrapAsCountVectorizer((JvmObjectReference)Reference.Invoke("setMinDF", value));

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
        /// <returns>Minimum term frequency</returns>
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
        /// <param name="value">Minimum term frequency</param>
        /// <returns>New <see cref="CountVectorizer"/> with the min term frequency set</returns>
        public CountVectorizer SetMinTF(double value) =>
            WrapAsCountVectorizer((JvmObjectReference)Reference.Invoke("setMinTF", value));

        /// <summary>
        /// Gets the max size of the vocabulary. <see cref="CountVectorizer"/> will build a
        /// vocabulary that only considers the top vocabSize terms ordered by term frequency across
        /// the corpus.
        /// </summary>
        /// <returns>The max size of the vocabulary of type int.</returns>
        public int GetVocabSize() => (int)Reference.Invoke("getVocabSize");

        /// <summary>
        /// Sets the max size of the vocabulary. <see cref="CountVectorizer"/> will build a
        /// vocabulary that only considers the top vocabSize terms ordered by term frequency across
        /// the corpus.
        /// </summary>
        /// <param name="value">The max vocabulary size</param>
        /// <returns><see cref="CountVectorizer"/> with the max vocab value set</returns>
        public CountVectorizer SetVocabSize(int value) =>
            WrapAsCountVectorizer(Reference.Invoke("setVocabSize", value));

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
        /// <returns>an <see cref="JavaMLReader&lt;CountVectorizer&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<CountVectorizer> Read() =>
            new JavaMLReader<CountVectorizer>((JvmObjectReference)Reference.Invoke("read"));

        private static CountVectorizer WrapAsCountVectorizer(object obj) =>
            new CountVectorizer((JvmObjectReference)obj);
    }
}
