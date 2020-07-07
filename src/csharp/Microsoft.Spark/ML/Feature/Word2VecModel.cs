// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    public class Word2VecModel : FeatureBase<Word2VecModel>, IJvmObjectReferenceProvider
    {
        static Word2VecModel()
        {
            ImplementingJavaClassName = "org.apache.spark.ml.feature.Word2VecModel";
        }

        /// <summary>
        /// Create a <see cref="Word2VecModel"/> without any parameters
        /// </summary>
        public Word2VecModel()
        {
        }

        /// <summary>
        /// Create a <see cref="Word2VecModel"/> with a UID that is used to give the
        /// <see cref="Word2VecModel"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Word2VecModel(string uid) : base(uid)
        {
        }
        
        internal Word2VecModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }
        
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Transform a sentence column to a vector column to represent the whole sentence.
        /// </summary>
        /// <param name="documentDF"><see cref="DataFrame"/> to transform</param>
        public DataFrame Transform(DataFrame documentDF) => 
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", documentDF));
        
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
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("findSynonyms", word, num));

        private static Word2VecModel WrapAsWord2VecModel(object obj) => 
            new Word2VecModel((JvmObjectReference)obj);
    }
}
