// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    public class Word2VecModel :
        JavaModel<Word2VecModel>,
        IJavaMLWritable,
        IJavaMLReadable<Word2VecModel>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.Word2VecModel";

        /// <summary>
        /// Create a <see cref="Word2VecModel"/> without any parameters
        /// </summary>
        public Word2VecModel() : base(s_className)
        {
        }

        /// <summary>
        /// Create a <see cref="Word2VecModel"/> with a UID that is used to give the
        /// <see cref="Word2VecModel"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Word2VecModel(string uid) : base(s_className, uid)
        {
        }

        internal Word2VecModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Transform a sentence column to a vector column to represent the whole sentence.
        /// </summary>
        /// <param name="documentDF"><see cref="DataFrame"/> to transform</param>
        public override DataFrame Transform(DataFrame documentDF) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", documentDF));

        /// <summary>
        /// Find <paramref name="num"/> number of words whose vector representation most similar to
        /// the supplied vector. If the supplied vector is the vector representation of a word in
        /// the model's vocabulary, that word will be in the results. Returns a dataframe with the
        /// words and the cosine similarities between the synonyms and the given word vector.
        /// </summary>
        /// <param name="word">The "word" to find similarities for, this can be a string or a
        /// vector representation.</param>
        /// <param name="num">The number of words to find that are similar to "word"</param>
        public DataFrame FindSynonyms(string word, int num) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("findSynonyms", word, num));

        /// <summary>
        /// Loads the <see cref="Word2VecModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">
        /// The path the previous <see cref="Word2VecModel"/> was saved to
        /// </param>
        /// <returns>New <see cref="Word2VecModel"/> object, loaded from path.</returns>
        public static Word2VecModel Load(string path) => WrapAsWord2VecModel(
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_className, "load", path));

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
        /// <returns>an <see cref="JavaMLReader&lt;Word2VecModel&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<Word2VecModel> Read() =>
            new JavaMLReader<Word2VecModel>((JvmObjectReference)Reference.Invoke("read"));

        private static Word2VecModel WrapAsWord2VecModel(object obj) =>
            new Word2VecModel((JvmObjectReference)obj);
    }
}
