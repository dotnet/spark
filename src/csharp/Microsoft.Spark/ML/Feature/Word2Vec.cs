// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    public class Word2Vec :
        JavaEstimator<Word2VecModel>,
        IJavaMLWritable,
        IJavaMLReadable<Word2Vec>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.Word2Vec";

        /// <summary>
        /// Create a <see cref="Word2Vec"/> without any parameters
        /// </summary>
        public Word2Vec() : base(s_className)
        {
        }

        /// <summary>
        /// Create a <see cref="Word2Vec"/> with a UID that is used to give the
        /// <see cref="Word2Vec"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Word2Vec(string uid) : base(s_className, uid)
        {
        }

        internal Word2Vec(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Gets the column that the <see cref="Word2Vec"/> should read from.
        /// </summary>
        /// <returns>The name of the input column.</returns>
        public string GetInputCol() => (string)(Reference.Invoke("getInputCol"));

        /// <summary>
        /// Sets the column that the <see cref="Word2Vec"/> should read from.
        /// </summary>
        /// <param name="value">The name of the column to as the source.</param>
        /// <returns><see cref="Word2Vec"/></returns>
        public Word2Vec SetInputCol(string value) =>
            WrapAsWord2Vec(Reference.Invoke("setInputCol", value));

        /// <summary>
        /// The <see cref="Word2Vec"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <returns>The name of the output column.</returns>
        public string GetOutputCol() => (string)(Reference.Invoke("getOutputCol"));

        /// <summary>
        /// The <see cref="Word2Vec"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the output column which will be created.</param>
        /// <returns>New <see cref="Word2Vec"/></returns>
        public Word2Vec SetOutputCol(string value) =>
            WrapAsWord2Vec(Reference.Invoke("setOutputCol", value));

        /// <summary>
        /// Gets the vector size, the dimension of the code that you want to transform from words.
        /// </summary>
        /// <returns>
        /// The vector size, the dimension of the code that you want to transform from words.
        /// </returns>
        public int GetVectorSize() => (int)(Reference.Invoke("getVectorSize"));

        /// <summary>
        /// Sets the vector size, the dimension of the code that you want to transform from words.
        /// </summary>
        /// <param name="value">
        /// The dimension of the code that you want to transform from words.
        /// </param>
        /// <returns><see cref="Word2Vec"/></returns>
        public Word2Vec SetVectorSize(int value) =>
            WrapAsWord2Vec(Reference.Invoke("setVectorSize", value));

        /// <summary>
        /// Gets the minimum number of times a token must appear to be included in the word2vec
        /// model's vocabulary.
        /// </summary>
        /// <returns>
        /// The minimum number of times a token must appear to be included in the word2vec model's
        /// vocabulary.
        /// </returns>
        public int GetMinCount() => (int)Reference.Invoke("getMinCount");

        /// <summary>
        /// The minimum number of times a token must appear to be included in the word2vec model's
        /// vocabulary.
        /// </summary>
        /// <param name="value">
        /// The minimum number of times a token must appear to be included in the word2vec model's
        /// vocabulary, the default is 5.
        /// </param>
        /// <returns><see cref="Word2Vec"/></returns>
        public virtual Word2Vec SetMinCount(int value) =>
            WrapAsWord2Vec(Reference.Invoke("setMinCount", value));

        /// <summary>Gets the maximum number of iterations.</summary>
        /// <returns>The maximum number of iterations.</returns>
        public int GetMaxIter() => (int)Reference.Invoke("getMaxIter");

        /// <summary>Maximum number of iterations (&gt;= 0).</summary>
        /// <param name="value">The number of iterations.</param>
        /// <returns><see cref="Word2Vec"/></returns>
        public Word2Vec SetMaxIter(int value) =>
            WrapAsWord2Vec(Reference.Invoke("setMaxIter", value));

        /// <summary>
        /// Gets the maximum length (in words) of each sentence in the input data.
        /// </summary>
        /// <returns>The maximum length (in words) of each sentence in the input data.</returns>
        public virtual int GetMaxSentenceLength() =>
            (int)Reference.Invoke("getMaxSentenceLength");

        /// <summary>
        /// Sets the maximum length (in words) of each sentence in the input data.
        /// </summary>
        /// <param name="value">
        /// The maximum length (in words) of each sentence in the input data.
        /// </param>
        /// <returns><see cref="Word2Vec"/></returns>
        public Word2Vec SetMaxSentenceLength(int value) =>
            WrapAsWord2Vec(Reference.Invoke("setMaxSentenceLength", value));

        /// <summary>Gets the number of partitions for sentences of words.</summary>
        /// <returns>The number of partitions for sentences of words.</returns>
        public int GetNumPartitions() => (int)Reference.Invoke("getNumPartitions");

        /// <summary>Sets the number of partitions for sentences of words.</summary>
        /// <param name="value">
        /// The number of partitions for sentences of words, default is 1.
        /// </param>
        /// <returns><see cref="Word2Vec"/></returns>
        public Word2Vec SetNumPartitions(int value) =>
            WrapAsWord2Vec(Reference.Invoke("setNumPartitions", value));

        /// <summary>Gets the value that is used for the random seed.</summary>
        /// <returns>The value that is used for the random seed.</returns>
        public long GetSeed() => (long)Reference.Invoke("getSeed");

        /// <summary>Random seed.</summary>
        /// <param name="value">The value to use for the random seed.</param>
        /// <returns><see cref="Word2Vec"/></returns>
        public Word2Vec SetSeed(long value) =>
            WrapAsWord2Vec(Reference.Invoke("setSeed", value));

        /// <summary>Gets the size to be used for each iteration of optimization.</summary>
        /// <returns>The size to be used for each iteration of optimization.</returns>
        public double GetStepSize() => (double)Reference.Invoke("getStepSize");

        /// <summary>Step size to be used for each iteration of optimization (&gt; 0).</summary>
        /// <param name="value">Value to use for the step size.</param>
        /// <returns><see cref="Word2Vec"/></returns>
        public Word2Vec SetStepSize(double value) =>
            WrapAsWord2Vec(Reference.Invoke("setStepSize", value));

        /// <summary>Gets the window size (context words from [-window, window]).</summary>
        /// <returns>The window size.</returns>
        public int GetWindowSize() => (int)Reference.Invoke("getWindowSize");

        /// <summary>The window size (context words from [-window, window]).</summary>
        /// <param name="value">
        /// The window size (context words from [-window, window]), default is 5.
        /// </param>
        /// <returns><see cref="Word2Vec"/></returns>
        public Word2Vec SetWindowSize(int value) =>
            WrapAsWord2Vec(Reference.Invoke("setWindowSize", value));

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataFrame">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="Word2VecModel"/></returns>
        public override Word2VecModel Fit(DataFrame dataFrame) =>
            new Word2VecModel((JvmObjectReference)Reference.Invoke("fit", dataFrame));

        /// <summary>
        /// Loads the <see cref="Word2Vec"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="Word2Vec"/> was saved to</param>
        /// <returns>New <see cref="Word2Vec"/> object, loaded from path.</returns>
        public static Word2Vec Load(string path) => WrapAsWord2Vec(
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
        /// <returns>an <see cref="JavaMLReader&lt;Word2Vec&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<Word2Vec> Read() =>
            new JavaMLReader<Word2Vec>((JvmObjectReference)Reference.Invoke("read"));

        private static Word2Vec WrapAsWord2Vec(object obj) =>
            new Word2Vec((JvmObjectReference)obj);
    }
}
