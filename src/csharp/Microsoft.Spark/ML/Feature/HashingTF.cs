// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
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
    public class HashingTF : IJvmObjectReferenceProvider
    {
       
        /// <summary>
        /// Create a <see cref="HashingTF"/> without any parameters
        /// </summary>
        public HashingTF()
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.ml.feature.HashingTF");
        }

        /// <summary>
        /// Create a <see cref="HashingTF"/> with a UID that is used to give the
        /// <see cref="HashingTF"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public HashingTF(string uid)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.ml.feature.HashingTF", uid);
        }
        
        internal HashingTF(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        private readonly JvmObjectReference _jvmObject;
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Sets the column that the <see cref="HashingTF"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns><see cref="HashingTF"/></returns>
        public HashingTF SetInputCol(string value)
        {
            return WrapAsHashingTF(_jvmObject.Invoke("setInputCol", value));
        }

        /// <summary>
        /// The <see cref="HashingTF"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column
        /// </param>
        /// <returns><see cref="HashingTF"/></returns>
        public HashingTF SetOutputCol(string value)
        {
            return WrapAsHashingTF(_jvmObject.Invoke("setOutputCol", value));
        }

        public HashingTF SetNumFeatures(int value)
        {
            return WrapAsHashingTF(_jvmObject.Invoke("setNumFeatures", value));
        }
        
        /// <summary>
        /// Executes the <see cref="HashingTF"/> and transforms the DataFrame to include the new
        /// column or columns with the tokens.
        /// </summary>
        /// <param name="source">The DataFrame to add the tokens to</param>
        /// <returns><see cref="DataFrame"/> containing the original data and the tokens</returns>
        public DataFrame Transform(DataFrame source)
        {
            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));
        }

        /// <summary>
        /// The reference we get back from each call isn't usable unless we wrap it in a new dotnet
        /// <see cref="HashingTF"/>
        /// </summary>
        /// <param name="obj">The <see cref="JvmObjectReference"/> to convert into a dotnet
        /// <see cref="HashingTF"/></param>
        /// <returns><see cref="HashingTF"/></returns>
        private static HashingTF WrapAsHashingTF(object obj)
        {
            return new HashingTF((JvmObjectReference)obj);
        }

        /// <summary>
        /// The uid that was used to create the <see cref="HashingTF"/>. If no UID is passed in
        /// when creating the <see cref="HashingTF"/> then a random UID is created when the
        /// <see cref="HashingTF"/> is created.
        /// </summary>
        /// <returns>string UID identifying the <see cref="HashingTF"/></returns>
        public string Uid()
        {
            return (string)_jvmObject.Invoke("uid");
        }
    }
}
