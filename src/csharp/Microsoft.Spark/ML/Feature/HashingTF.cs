// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// A <see cref="HashingTF"/> Maps a sequence of terms to their term frequencies using the
    /// hashing trick. Currently we use Austin Appleby's MurmurHash 3 algorithm
    /// (MurmurHash3_x86_32) to calculate the hash code value for the term object. Since a simple
    /// modulo is used to transform the hash function to a column index, it is advisable to use a
    /// power of two as the numFeatures parameter; otherwise the features will not be mapped evenly
    /// to the columns.
    /// </summary>
    public class HashingTF :
        JavaTransformer,
        IJavaMLWritable,
        IJavaMLReadable<HashingTF>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.HashingTF";

        /// <summary>
        /// Create a <see cref="HashingTF"/> without any parameters
        /// </summary>
        public HashingTF() : base(s_className)
        {
        }

        /// <summary>
        /// Create a <see cref="HashingTF"/> with a UID that is used to give the
        /// <see cref="HashingTF"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public HashingTF(string uid) : base(s_className, uid)
        {
        }

        internal HashingTF(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Loads the <see cref="HashingTF"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">The path the previous <see cref="HashingTF"/> was saved to</param>
        /// <returns>New <see cref="HashingTF"/> object</returns>
        public static HashingTF Load(string path) =>
            WrapAsHashingTF(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_className, "load", path));

        /// <summary>
        /// Gets the binary toggle that controls term frequency counts
        /// </summary>
        /// <returns>Flag showing whether the binary toggle is on or off</returns>
        public bool GetBinary() => (bool)Reference.Invoke("getBinary");

        /// <summary>
        /// Binary toggle to control term frequency counts.
        /// If true, all non-zero counts are set to 1.  This is useful for discrete probabilistic
        /// models that model binary events rather than integer counts
        ///</summary>
        /// <param name="value">binary toggle, default is false</param>
        public HashingTF SetBinary(bool value) =>
            WrapAsHashingTF(Reference.Invoke("setBinary", value));

        /// <summary>
        /// Gets the column that the <see cref="HashingTF"/> should read from
        /// </summary>
        /// <returns>string, the name of the input column</returns>
        public string GetInputCol() => (string)Reference.Invoke("getInputCol");

        /// <summary>
        /// Sets the column that the <see cref="HashingTF"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="HashingTF"/> object</returns>
        public HashingTF SetInputCol(string value) =>
            WrapAsHashingTF(Reference.Invoke("setInputCol", value));

        /// <summary>
        /// The <see cref="HashingTF"/> will create a new column in the <see cref="DataFrame"/>,
        /// this is the name of the new column.
        /// </summary>
        /// <returns>string, the name of the output col</returns>
        public string GetOutputCol() => (string)Reference.Invoke("getOutputCol");

        /// <summary>
        /// The <see cref="HashingTF"/> will create a new column in the <see cref="DataFrame"/>,
        /// this is the name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column</param>
        /// <returns>New <see cref="HashingTF"/> object</returns>
        public HashingTF SetOutputCol(string value) =>
            WrapAsHashingTF(Reference.Invoke("setOutputCol", value));

        /// <summary>
        /// Gets the number of features that should be used. Since a simple modulo is used to
        /// transform the hash function to a column index, it is advisable to use a power of two
        /// as the numFeatures parameter; otherwise the features will not be mapped evenly to the
        /// columns.
        /// </summary>
        /// <returns>The number of features to be used</returns>
        public int GetNumFeatures() => (int)Reference.Invoke("getNumFeatures");

        /// <summary>
        /// Sets the number of features that should be used. Since a simple modulo is used to
        /// transform the hash function to a column index, it is advisable to use a power of two as
        /// the numFeatures parameter; otherwise the features will not be mapped evenly to the
        /// columns.
        /// </summary>
        /// <param name="value">int</param>
        /// <returns>New <see cref="HashingTF"/> object</returns>
        public HashingTF SetNumFeatures(int value) =>
            WrapAsHashingTF(Reference.Invoke("setNumFeatures", value));

        /// <summary>
        /// Executes the <see cref="HashingTF"/> and transforms the DataFrame to include the new
        /// column or columns with the tokens.
        /// </summary>
        /// <param name="source">The <see cref="DataFrame"/> to add the tokens to</param>
        /// <returns><see cref="DataFrame"/> containing the original data and the tokens</returns>
        public override DataFrame Transform(DataFrame source) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", source));

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
        /// <returns>an <see cref="JavaMLReader&lt;HashingTF&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<HashingTF> Read() =>
            new JavaMLReader<HashingTF>((JvmObjectReference)Reference.Invoke("read"));

        private static HashingTF WrapAsHashingTF(object obj) =>
            new HashingTF((JvmObjectReference)obj);
    }
}
