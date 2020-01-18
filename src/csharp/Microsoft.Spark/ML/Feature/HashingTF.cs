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
    public class HashingTF : IJvmObjectReferenceProvider
    {
       
        /// <summary>
        /// Create a <see cref="HashingTF"/> without any parameters
        /// </summary>
        public HashingTF()
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(_javaClassName);
        }

        /// <summary>
        /// Create a <see cref="HashingTF"/> with a UID that is used to give the
        /// <see cref="HashingTF"/> a unique ID
        /// <param name="uid">unique identifier</param>
        /// </summary>
        public HashingTF(string uid)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(_javaClassName, uid);
        }
        
        internal HashingTF(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        private readonly JvmObjectReference _jvmObject;
        private const string _javaClassName = "org.apache.spark.ml.feature.HashingTF";
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Loads the <see cref="HashingTF"/> that was previously saved using Save
        /// </summary>
        /// <param name="path"></param>
        /// <returns><see cref="HashingTF"/></returns>
        public static HashingTF Load(string path)
        {
            return WrapAsHashingTF(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(_javaClassName,"load", path));
        }
        
        /// <summary>
        /// Saves the <see cref="HashingTF"/> so that it can be loaded later using Load
        /// </summary>
        /// <param name="path"></param>
        /// <returns><see cref="HashingTF"/></returns>
        public HashingTF Save(string path)
        {
            return WrapAsHashingTF(_jvmObject.Invoke("save", path));
        }
        
        /// <summary>
        /// Gets the binary toggle that controls term frequency counts
        /// </summary>
        /// <returns>bool</returns>
        public bool GetBinary()
        {
            return (bool)_jvmObject.Invoke("getBinary");
        }

        /// <summary>
        /// Binary toggle to control term frequency counts.
        /// If true, all non-zero counts are set to 1.  This is useful for discrete probabilistic
        /// models that model binary events rather than integer counts
        ///</summary>
        /// <param name="value">binary toggle, default is false</param>
        public HashingTF SetBinary(bool value)
        {
            return WrapAsHashingTF(_jvmObject.Invoke("setBinary", value));
        }
        
        /// <summary>
        /// Gets the column that the <see cref="HashingTF"/> should read from
        /// </summary>
        /// <returns>string, the name of the input column</returns>
        public string GetInputCol()
        {
            return (string)_jvmObject.Invoke("getInputCol");
        }
        
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
        /// The <see cref="HashingTF"/> will create a new column in the <see cref="DataFrame"/>,
        /// this is the name of the new column.
        /// </summary>
        /// <returns>string, the name of the output col</returns>
        public string GetOutputCol()
        {
            return (string)_jvmObject.Invoke("getOutputCol");
        }
        
        /// <summary>
        /// The <see cref="HashingTF"/> will create a new column in the <see cref="DataFrame"/>,
        /// this is the name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column</param>
        /// <returns><see cref="HashingTF"/></returns>
        public HashingTF SetOutputCol(string value)
        {
            return WrapAsHashingTF(_jvmObject.Invoke("setOutputCol", value));
        }

        /// <summary>
        /// Gets the number of features that should be used
        /// </summary>
        /// <returns>int</returns>
        public int GetNumFeatures()
        {
            return (int)_jvmObject.Invoke("getNumFeatures");
        }
        
        /// <summary>
        /// Sets the number of features that should be used
        /// </summary>
        /// <param name="value">int</param>
        /// <returns><see cref="HashingTF"/></returns>
        public HashingTF SetNumFeatures(int value)
        {
            return WrapAsHashingTF(_jvmObject.Invoke("setNumFeatures", value));
        }

        /// <summary>
        /// An immutable unique ID for the object and its derivatives.
        /// </summary>
        /// <returns>string</returns>
        public string Uid()
        {
            return (string)_jvmObject.Invoke("uid");
        }

        /// <summary>
        /// Executes the <see cref="HashingTF"/> and transforms the DataFrame to include the new
        /// column or columns with the tokens.
        /// </summary>
        /// <param name="source">The <see cref="DataFrame"/> to add the tokens to</param>
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
    }
}
