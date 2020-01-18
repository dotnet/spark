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
    /// A <see cref="Tokenizer"/> that converts the input string to lowercase and then splits it by
    /// white spaces.
    /// </summary>
    public class Tokenizer : IJvmObjectReferenceProvider
    {
       
        /// <summary>
        /// Create a <see cref="Tokenizer"/> without any parameters
        /// </summary>
        public Tokenizer()
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(JavaClassName);
        }

        /// <summary>
        /// Create a <see cref="Tokenizer"/> with a UID that is used to give the
        /// <see cref="Tokenizer"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Tokenizer(string uid)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(JavaClassName, uid);
        }
        
        internal Tokenizer(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        private readonly JvmObjectReference _jvmObject;
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        private const string JavaClassName = "org.apache.spark.ml.feature.Tokenizer";
        
        /// <summary>
        /// Gets the column that the <see cref="Tokenizer"/> should read from
        /// </summary>
        /// <returns>string, input column</returns>
        public string GetInputCol()
        {
            return (string)(_jvmObject.Invoke("getInputCol"));
        }
        
        /// <summary>
        /// Sets the column that the <see cref="Tokenizer"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns><see cref="Tokenizer"/></returns>
        public Tokenizer SetInputCol(string value)
        {
            return WrapAsTokenizer(_jvmObject.Invoke("setInputCol", value));
        }

        /// <summary>
        /// The <see cref="Tokenizer"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol()
        {
            return (string)(_jvmObject.Invoke("getOutputCol"));
        }
        
        /// <summary>
        /// The <see cref="Tokenizer"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column
        /// </param>
        /// <returns><see cref="Tokenizer"/></returns>
        public Tokenizer SetOutputCol(string value)
        {
            return WrapAsTokenizer(_jvmObject.Invoke("setOutputCol", value));
        }
        
        /// <summary>
        /// Executes the <see cref="Tokenizer"/> and transforms the DataFrame to include the new
        /// column
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns><see cref="DataFrame"/></returns>
        public DataFrame Transform(DataFrame source)
        {
            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));
        }

        /// <summary>
        /// The reference we get back from each call isn't usable unless we wrap it in a new dotnet
        /// <see cref="Tokenizer"/>
        /// </summary>
        /// <param name="obj">The <see cref="JvmObjectReference"/> to convert into a dotnet
        /// <see cref="Tokenizer"/></param>
        /// <returns><see cref="Tokenizer"/></returns>
        private static Tokenizer WrapAsTokenizer(object obj)
        {
            return new Tokenizer((JvmObjectReference)obj);
        }

        /// <summary>
        /// The uid that was used to create the <see cref="Tokenizer"/>. If no UID is passed in
        /// when creating the <see cref="Tokenizer"/> then a random UID is created when the
        /// <see cref="Tokenizer"/> is created.
        /// </summary>
        /// <returns>string UID identifying the <see cref="Tokenizer"/></returns>
        public string Uid()
        {
            return (string)_jvmObject.Invoke("uid");
        }
        
        /// <summary>
        /// Loads the <see cref="Tokenizer"/> that was previously saved using Save
        /// </summary>
        /// <param name="path"></param>
        /// <returns><see cref="Tokenizer"/></returns>
        public static Tokenizer Load(string path)
        {
            return WrapAsTokenizer(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(JavaClassName, "load", path));
        }
        
        /// <summary>
        /// Saves the <see cref="Tokenizer"/> so that it can be loaded later using Load
        /// </summary>
        /// <param name="path"></param>
        /// <returns><see cref="Tokenizer"/></returns>
        public Tokenizer Save(string path)
        {
            return WrapAsTokenizer(_jvmObject.Invoke("save", path));
        }
    }
}
