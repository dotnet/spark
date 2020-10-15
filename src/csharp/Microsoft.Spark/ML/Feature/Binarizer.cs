// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// A <see cref="Binarizer"/>, Binarize a column of continuous features given a threshold.
    /// </summary>
    public class Binarizer : FeatureBase<Binarizer>, IJvmObjectReferenceProvider
    {
        private static readonly string s_binarizerClassName =
            "org.apache.spark.ml.feature.Binarizer";

        public Binarizer() : base(s_binarizerClassName)
        {
        }

        public Binarizer(string uid) : base(s_binarizerClassName, uid)
        {
        }

        internal Binarizer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
        
        /// <summary>
        /// Gets the column that the <see cref="Binarizer"/> should read from
        /// </summary>
        /// <returns>string, input column</returns>
        public string GetInputCol() => (string)(_jvmObject.Invoke("getInputCol"));

        /// <summary>
        /// Sets the column that the <see cref="Binarizer"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="Binarizer"/> object</returns>
        public Binarizer SetInputCol(string value) => 
            WrapAsBinarizer(_jvmObject.Invoke("setInputCol", value));

        /// <summary>
        /// The <see cref="Binarizer"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol() => (string)(_jvmObject.Invoke("getOutputCol"));

        /// <summary>
        /// The <see cref="Binarizer"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column</param>
        /// <returns>New <see cref="Binarizer"/> object</returns>
        public Binarizer SetOutputCol(string value) => 
            WrapAsBinarizer(_jvmObject.Invoke("setOutputCol", value));

        /// <summary>
        /// Executes the <see cref="Binarizer"/> and transforms the DataFrame to include the new
        /// column
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns>
        /// New <see cref="DataFrame"/> object with the source <see cref="DataFrame"/> transformed
        /// </returns>
        public DataFrame Transform(DataFrame source) => 
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));

        /// <summary>
        /// Loads the <see cref="Binarizer"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">The path the previous <see cref="Binarizer"/> was saved to</param>
        /// <returns>New <see cref="Binarizer"/> object, loaded from path</returns>
        public static Binarizer Load(string path)
        {
            return WrapAsBinarizer(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_binarizerClassName, "load", path));
        }
        
        private static Binarizer WrapAsBinarizer(object obj) => 
            new Binarizer((JvmObjectReference)obj);
    }
}
