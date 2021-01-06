// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// <see cref="StringIndexer"/> encodes a string column of labels to a column of label indices.
    /// </summary>
    public class StringIndexer : FeatureBase<StringIndexer>, IJvmObjectReferenceProvider
    {
        private static readonly string s_StringIndexerClassName =
            "org.apache.spark.ml.feature.StringIndexer";

        /// <summary>
        /// Create a <see cref="StringIndexer"/> without any parameters.
        /// </summary>
        public StringIndexer() : base(s_StringIndexerClassName)
        {
        }

        /// <summary>
        /// Create a <see cref="StringIndexer"/> with a UID that is used to give the
        /// <see cref="StringIndexer"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public StringIndexer(string uid) : base(s_StringIndexerClassName, uid)
        {
        }

        internal StringIndexer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Executes the <see cref="StringIndexer"/> and transforms the schema.
        /// </summary>
        /// <param name="value">The Schema to be transformed</param>
        /// <returns>
        /// New <see cref="StructType"/> object with the schema <see cref="StructType"/> transformed.
        /// </returns>
        public StructType TransformSchema(StructType value) =>
            new StructType(
                (JvmObjectReference)_jvmObject.Invoke(
                    "transformSchema",
                    DataType.FromJson(_jvmObject.Jvm, value.Json)));

        /// <summary>
        /// Executes the <see cref="StringIndexer"/> and fits a model to the input data.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public StringIndexerModel Fit(DataFrame source) =>
            new StringIndexerModel((JvmObjectReference)_jvmObject.Invoke("fit", source));

        /// <summary>
        /// Gets the HandleInvalid.
        /// </summary>
        /// <returns>Handle Invalid option</returns>
        public string GetHandleInvalid() => (string)_jvmObject.Invoke("handleInvalid");

        /// <summary>
        /// Sets the Handle Invalid option to <see cref="StringIndexer"/>.
        /// </summary>
        /// <param name="handleInvalid">Handle Invalid option</param>
        /// <returns>
        /// <see cref="StringIndexer"/> with the Handle Invalid set.
        /// </returns>
        public StringIndexer SetHandleInvalid(string handleInvalid) =>
            WrapAsStringIndexer((JvmObjectReference)_jvmObject.Invoke("setHandleInvalid", handleInvalid));

        /// <summary>
        /// Gets the InputCol.
        /// </summary>
        /// <returns>Input Col option</returns>
        public string GetInputCol() => (string)_jvmObject.Invoke("inputCol");

        /// <summary>
        /// Sets the Input Col option to <see cref="StringIndexer"/>.
        /// </summary>
        /// <param name="inputCol">Input Col option</param>
        /// <returns>
        /// <see cref="StringIndexer"/> with the Input Col set.
        /// </returns>
        public StringIndexer SetInputCol(string inputCol) =>
            WrapAsStringIndexer((JvmObjectReference)_jvmObject.Invoke("setInputCol", inputCol));

        /// <summary>
        /// Gets the InputCols array.
        /// </summary>
        /// <returns>Input Cols array option</returns>
        public string[] GetInputCols() => (string[])_jvmObject.Invoke("inputCols");

        /// <summary>
        /// Sets the Input Cols array option to <see cref="StringIndexer"/>.
        /// </summary>
        /// <param name="inputCols">Input Cols array option</param>
        /// <returns>
        /// <see cref="StringIndexer"/> with the Input Cols array set.
        /// </returns>
        public StringIndexer SetInputCols(string[] inputCols) =>
            WrapAsStringIndexer((JvmObjectReference)_jvmObject.Invoke("setInputCol", inputCols));

        /// <summary>
        /// Gets the OutputCol.
        /// </summary>
        /// <returns>Output Col option</returns>
        public string GetOutputCol() => (string)_jvmObject.Invoke("outputCol");

        /// <summary>
        /// Sets the Output Col option to <see cref="StringIndexer"/>.
        /// </summary>
        /// <param name="outputCol">Output Col option</param>
        /// <returns>
        /// <see cref="StringIndexer"/> with the Output Col set.
        /// </returns>
        public StringIndexer SetOutputCol(string outputCol) =>
            WrapAsStringIndexer((JvmObjectReference)_jvmObject.Invoke("setOutputCol", outputCol));

        /// <summary>
        /// Gets the OutputCols array.
        /// </summary>
        /// <returns>Output Cols array option</returns>
        public string[] GetOutputCols() => (string[])_jvmObject.Invoke("outputCols");

        /// <summary>
        /// Sets the Output Cols array option to <see cref="StringIndexer"/>.
        /// </summary>
        /// <param name="outputCols">Output Cols array option</param>
        /// <returns>
        /// <see cref="StringIndexer"/> with the Output Cols array set.
        /// </returns>
        public StringIndexer SetOutputCols(string[] outputCols) =>
            WrapAsStringIndexer((JvmObjectReference)_jvmObject.Invoke("setOutputCol", outputCols));

        /// <summary>
        /// Gets the String Order Type.
        /// </summary>
        /// <returns>String Order Type</returns>
        public string GetStringOrderType() => (string)_jvmObject.Invoke("stringOrderType");

        /// <summary>
        /// Sets the String Order Type to <see cref="StringIndexer"/>.
        /// </summary>
        /// <param name="stringOrderType">String Order Type</param>
        /// <returns>
        /// <see cref="StringIndexer"/> with the String Order Type set.
        /// </returns>
        public StringIndexer SetStringOrderType(string stringOrderType) =>
            WrapAsStringIndexer((JvmObjectReference)_jvmObject.Invoke("setStringOrderType", stringOrderType));

        /// <summary>
        /// Loads the <see cref="StringIndexer"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">The path the previous <see cref="StringIndexer"/> was saved to</param>
        /// <returns>New <see cref="StringIndexer"/> object, loaded from path</returns>
        public static StringIndexer Load(string path) =>
            WrapAsStringIndexer(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_StringIndexerClassName,
                    "load",
                    path));

        private static StringIndexer WrapAsStringIndexer(object obj) =>
            new StringIndexer((JvmObjectReference)obj);
    }
}
