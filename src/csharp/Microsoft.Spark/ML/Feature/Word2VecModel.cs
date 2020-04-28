// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    public class Word2VecModel : IJvmObjectReferenceProvider
    {
        private static readonly string s_word2VecModelClassName = 
            "org.apache.spark.ml.feature.Word2VecModel";
        
        private readonly JvmObjectReference _jvmObject;

        /// <summary>
        /// Create a <see cref="Word2VecModel"/> without any parameters
        /// </summary>
        public Word2VecModel()
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(s_word2VecModelClassName);
        }

        /// <summary>
        /// Create a <see cref="Word2VecModel"/> with a UID that is used to give the
        /// <see cref="Word2VecModel"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Word2VecModel(string uid)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(s_word2VecModelClassName, uid);
        }
        
        internal Word2VecModel(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }
        
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Transform a sentence column to a vector column to represent the whole sentence.
        /// </summary>
        /// <param name="documentDF"><see cref="Word2VecModel"/> to transform</param>
        public DataFrame Transform(DataFrame documentDF) => 
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", documentDF));
        
        /// <summary>
        /// Find <paramref name="num"/> number of words whose vector representation most similar to the supplied
        /// vector. If the supplied vector is the vector representation of a word in the model's
        /// vocabulary, that word will be in the results. Returns a dataframe with the words and
        /// the cosine similarities between the synonyms and the given word vector.
        /// </summary>
        /// <param name="word">The "word" to find similarities for, this can be a string or a
        /// vector representation.</param>
        /// <param name="num">The number of words to find that are similar to "word"</param>
        public DataFrame FindSynonyms(string word, int num) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("findSynonyms", word, num));
        
        /// <summary>
        /// Loads the <see cref="Word2VecModel"/> that was previously saved using
        /// <see cref="Save(string)"/>.
        /// </summary>
        /// <param name="path">
        /// The path the previous <see cref="Word2VecModel"/> was saved to
        /// </param>
        /// <returns>New <see cref="Word2VecModel"/> object, loaded from path.</returns>
        public static Word2VecModel Load(string path) => WrapAsWord2VecModel(
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_word2VecModelClassName, "load", path));
        
        /// <summary>
        /// Saves the <see cref="Word2VecModel"/> so that it can be loaded later using
        /// <see cref="Load(string)"/>.
        /// </summary>
        /// <param name="path">The path to save the <see cref="Word2VecModel"/> to.</param>
        /// <returns>New <see cref="Word2VecModel"/> object.</returns>
        public Word2VecModel Save(string path) => 
            WrapAsWord2VecModel(_jvmObject.Invoke("save", path));
        
        /// <summary>
        /// The UID that was used to create the <see cref="Word2Vec"/>. If no UID is passed in
        /// when creating the <see cref="Word2Vec"/> then a random UID is created when the
        /// <see cref="Word2Vec"/> is created.
        /// </summary>
        /// <returns>string UID identifying the <see cref="Word2Vec"/>.</returns>
        public string Uid() => (string)_jvmObject.Invoke("uid");

        private static Word2VecModel WrapAsWord2VecModel(object obj) => 
            new Word2VecModel((JvmObjectReference)obj);
    }
}
