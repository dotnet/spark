// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    public class CountVectorizerModel
        : FeatureBase<CountVectorizerModel>, IJvmObjectReferenceProvider
    {
        private static readonly string s_countVectorizerModelClassName = 
            "org.apache.spark.ml.feature.CountVectorizerModel";
        
        /// <summary>
        /// Creates a <see cref="CountVectorizerModel"/> without any parameters
        /// </summary>
        /// <param name="vocabulary">The vocabulary to use</param>
        public CountVectorizerModel(List<string> vocabulary) 
            : this(SparkEnvironment.JvmBridge.CallConstructor(
                s_countVectorizerModelClassName, vocabulary))
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
                s_countVectorizerModelClassName, uid, vocabulary))
        {
        }
        
        internal CountVectorizerModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
        
        /// <summary>
        /// Loads the <see cref="CountVectorizerModel"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">
        /// The path the previous <see cref="CountVectorizerModel"/> was saved to
        /// </param>
        /// <returns>New <see cref="CountVectorizerModel"/> object</returns>
        public static CountVectorizerModel Load(string path) =>
            WrapAsCountVectorizerModel((JvmObjectReference)
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_countVectorizerModelClassName, "load", path));
        
        /// <summary>
        /// Gets the binary toggle to control the output vector values. If True, all nonzero counts
        /// (after minTF filter applied) are set to 1. This is useful for discrete probabilistic
        /// models that model binary events rather than integer counts. Default: false
        /// </summary>
        /// <returns>Toggle value of type boolean</returns>
        public bool GetBinary() => (bool)_jvmObject.Invoke("getBinary");

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
            WrapAsCountVectorizerModel((JvmObjectReference)_jvmObject.Invoke("setBinary", value));

        /// <summary>
        /// Gets the column that the <see cref="CountVectorizerModel"/> should read from and
        /// convert into buckets. This would have been set by SetInputCol
        /// </summary>
        /// <returns>string, the input column</returns>
        public string GetInputCol() => (string)_jvmObject.Invoke("getInputCol");
        
        /// <summary>
        /// Sets the column that the <see cref="CountVectorizerModel"/> should read from.
        /// </summary>
        /// <param name="value">The name of the column to use as the source.</param>
        /// <returns><see cref="CountVectorizerModel"/> with the input column set</returns>
        public CountVectorizerModel SetInputCol(string value) =>
            WrapAsCountVectorizerModel(
                (JvmObjectReference)_jvmObject.Invoke("setInputCol", value));
        
        /// <summary>
        /// Gets the name of the new column the <see cref="CountVectorizerModel"/> will create in
        /// the DataFrame.
        /// </summary>
        /// <returns>The name of the output column.</returns>
        public string GetOutputCol() => (string)_jvmObject.Invoke("getOutputCol");
        
        /// <summary>
        /// Sets the name of the new column the <see cref="CountVectorizerModel"/> will create in
        /// the DataFrame.
        /// </summary>
        /// <param name="value">The name of the output column which will be created.</param>
        /// <returns>New <see cref="CountVectorizerModel"/> with the output column set</returns>
        public CountVectorizerModel SetOutputCol(string value) =>
            WrapAsCountVectorizerModel(
                (JvmObjectReference)_jvmObject.Invoke("setOutputCol", value));
        
        /// <summary>
        /// Gets the maximum number of different documents a term could appear in to be included in
        /// the vocabulary. A term that appears more than the threshold will be ignored. If this is
        /// an integer greater than or equal to 1, this specifies the maximum number of documents
        /// the term could appear in; if this is a double in [0,1), then this specifies the maximum
        /// fraction of documents the term could appear in.
        /// </summary>
        /// <returns>The maximum document term frequency of type double.</returns>
        public double GetMaxDF() => (double)_jvmObject.Invoke("getMaxDF");
        
        /// <summary>
        /// Gets the minimum number of different documents a term must appear in to be included in
        /// the vocabulary. If this is an integer greater than or equal to 1, this specifies the
        /// number of documents the term must appear in; if this is a double in [0,1), then this
        /// specifies the fraction of documents.
        /// </summary>
        /// <returns>The minimum document term frequency</returns>
        public double GetMinDF() => (double)_jvmObject.Invoke("getMinDF");

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
        public double GetMinTF() => (double)_jvmObject.Invoke("getMinTF");

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
            WrapAsCountVectorizerModel((JvmObjectReference)_jvmObject.Invoke("setMinTF", value));
        
        /// <summary>
        /// Gets the max size of the vocabulary. <see cref="CountVectorizerModel"/> will build a
        /// vocabulary that only considers the top vocabSize terms ordered by term frequency across
        /// the corpus.
        /// </summary>
        /// <returns>The max size of the vocabulary of type int.</returns>
        public int GetVocabSize() => (int)_jvmObject.Invoke("getVocabSize");
        
        private static CountVectorizerModel WrapAsCountVectorizerModel(object obj) => 
            new CountVectorizerModel((JvmObjectReference)obj);
    }
}
