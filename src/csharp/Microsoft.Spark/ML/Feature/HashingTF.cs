// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

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
    public class HashingTF : FeatureBase<HashingTF>, IJvmObjectReferenceProvider
    {
        private static readonly string s_hashingTfClassName = 
            "org.apache.spark.ml.feature.HashingTF";

        /// <summary>
        /// Create a <see cref="HashingTF"/> without any parameters
        /// </summary>
        public HashingTF() : base(s_hashingTfClassName)
        {
        }

        /// <summary>
        /// Create a <see cref="HashingTF"/> with a UID that is used to give the
        /// <see cref="HashingTF"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public HashingTF(string uid) : base(s_hashingTfClassName, uid)
        {
        }
        
        internal HashingTF(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Loads the <see cref="HashingTF"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">The path the previous <see cref="HashingTF"/> was saved to</param>
        /// <returns>New <see cref="HashingTF"/> object</returns>
        public static HashingTF Load(string path) =>
            WrapAsHashingTF(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_hashingTfClassName, "load", path));

        /// <summary>
        /// Gets the binary toggle that controls term frequency counts
        /// </summary>
        /// <returns>Flag showing whether the binary toggle is on or off</returns>
        public bool GetBinary() => (bool)_jvmObject.Invoke("getBinary");

        /// <summary>
        /// Binary toggle to control term frequency counts.
        /// If true, all non-zero counts are set to 1.  This is useful for discrete probabilistic
        /// models that model binary events rather than integer counts
        ///</summary>
        /// <param name="value">binary toggle, default is false</param>
        public HashingTF SetBinary(bool value) => 
            WrapAsHashingTF(_jvmObject.Invoke("setBinary", value));

        /// <summary>
        /// Gets the column that the <see cref="HashingTF"/> should read from
        /// </summary>
        /// <returns>string, the name of the input column</returns>
        public string GetInputCol() => (string)_jvmObject.Invoke("getInputCol");

        /// <summary>
        /// Sets the column that the <see cref="HashingTF"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="HashingTF"/> object</returns>
        public HashingTF SetInputCol(string value) => 
            WrapAsHashingTF(_jvmObject.Invoke("setInputCol", value));

        /// <summary>
        /// The <see cref="HashingTF"/> will create a new column in the <see cref="DataFrame"/>,
        /// this is the name of the new column.
        /// </summary>
        /// <returns>string, the name of the output col</returns>
        public string GetOutputCol() => (string)_jvmObject.Invoke("getOutputCol");

        /// <summary>
        /// The <see cref="HashingTF"/> will create a new column in the <see cref="DataFrame"/>,
        /// this is the name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column</param>
        /// <returns>New <see cref="HashingTF"/> object</returns>
        public HashingTF SetOutputCol(string value) => 
            WrapAsHashingTF(_jvmObject.Invoke("setOutputCol", value));

        /// <summary>
        /// Gets the number of features that should be used. Since a simple modulo is used to
        /// transform the hash function to a column index, it is advisable to use a power of two
        /// as the numFeatures parameter; otherwise the features will not be mapped evenly to the
        /// columns.
        /// </summary>
        /// <returns>The number of features to be used</returns>
        public int GetNumFeatures() => (int)_jvmObject.Invoke("getNumFeatures");

        /// <summary>
        /// Sets the number of features that should be used. Since a simple modulo is used to
        /// transform the hash function to a column index, it is advisable to use a power of two as
        /// the numFeatures parameter; otherwise the features will not be mapped evenly to the
        /// columns.
        /// </summary>
        /// <param name="value">int</param>
        /// <returns>New <see cref="HashingTF"/> object</returns>
        public HashingTF SetNumFeatures(int value) => 
            WrapAsHashingTF(_jvmObject.Invoke("setNumFeatures", value));

        /// <summary>
        /// Executes the <see cref="HashingTF"/> and transforms the DataFrame to include the new
        /// column or columns with the tokens.
        /// </summary>
        /// <param name="source">The <see cref="DataFrame"/> to add the tokens to</param>
        /// <returns><see cref="DataFrame"/> containing the original data and the tokens</returns>
        public DataFrame Transform(DataFrame source) => 
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));

        private static HashingTF WrapAsHashingTF(object obj) => 
            new HashingTF((JvmObjectReference)obj);
    }
}
