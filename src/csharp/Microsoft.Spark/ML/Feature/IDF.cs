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
    /// Inverse document frequency (IDF). The standard formulation is used:
    ///     idf = log((m + 1) / (d(t) + 1)), where m is the total number of documents and d(t) is
    /// the number of documents that contain term t.
    /// 
    /// This implementation supports filtering out terms which do not appear in a minimum number
    /// of documents (controlled by the variable minDocFreq). For terms that are not in at least
    /// minDocFreq documents, the IDF is found as 0, resulting in TF-IDFs of 0.
    /// </summary>
    public class IDF : IJvmObjectReferenceProvider
    {
       
        /// <summary>
        /// Create a <see cref="IDF"/> without any parameters
        /// </summary>
        public IDF()
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.ml.feature.IDF");
        }

        /// <summary>
        /// Create a <see cref="IDF"/> with a UID that is used to give the
        /// <see cref="IDF"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public IDF(string uid)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.ml.feature.IDF", uid);
        }
        
        internal IDF(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        private readonly JvmObjectReference _jvmObject;
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Sets the column that the <see cref="IDF"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns><see cref="IDF"/></returns>
        public IDF SetInputCol(string value)
        {
            return WrapAsIDF(_jvmObject.Invoke("setInputCol", value));
        }

        /// <summary>
        /// The <see cref="IDF"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column
        /// </param>
        /// <returns><see cref="IDF"/></returns>
        public IDF SetOutputCol(string value)
        {
            return WrapAsIDF(_jvmObject.Invoke("setOutputCol", value));
        }

        /// <summary>
        /// Minimum of documents in which a term should appear for filtering
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public IDF SetMinDocFreq(int value)
        {
            return WrapAsIDF(_jvmObject.Invoke("setMinDocFreq", value));
        }
        
        /// <summary>
        /// Fits a model to the input data.
        /// </summary>
        /// <param name="source">The DataFrame to fit the model to</param>
        /// <returns><see cref="IDFModel"/></returns>
        public IDFModel Fit(DataFrame source)
        {
            return new IDFModel((JvmObjectReference)_jvmObject.Invoke("fit", source));
        }

        /// <summary>
        /// The reference we get back from each call isn't usable unless we wrap it in a new dotnet
        /// <see cref="IDF"/>
        /// </summary>
        /// <param name="obj">The <see cref="JvmObjectReference"/> to convert into a dotnet
        /// <see cref="IDF"/></param>
        /// <returns><see cref="IDF"/></returns>
        private static IDF WrapAsIDF(object obj)
        {
            return new IDF((JvmObjectReference)obj);
        }

        /// <summary>
        /// The uid that was used to create the <see cref="IDF"/>. If no UID is passed in
        /// when creating the <see cref="IDF"/> then a random UID is created when the
        /// <see cref="IDF"/> is created.
        /// </summary>
        /// <returns>string UID identifying the <see cref="IDF"/></returns>
        public string Uid()
        {
            return (string)_jvmObject.Invoke("uid");
        }
    }
}
