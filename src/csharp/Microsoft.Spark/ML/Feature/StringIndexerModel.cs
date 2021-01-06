// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.ML.Feature
{
    public class StringIndexerModel
        : FeatureBase<StringIndexerModel>, IJvmObjectReferenceProvider
    {
        private static readonly string s_stringIndexerModelClassName =
            "org.apache.spark.ml.feature.StringIndexerModel";

        /// <summary>
        /// Creates a <see cref="StringIndexerModel"/> without any parameters
        /// </summary>
        /// <param name="vocabulary">The vocabulary to use</param>
        public StringIndexerModel(List<string> vocabulary)
            : this(SparkEnvironment.JvmBridge.CallConstructor(
                s_stringIndexerModelClassName, vocabulary))
        {
        }

        /// <summary>
        /// Creates a <see cref="StringIndexerModel"/> with a UID that is used to give the
        /// <see cref="StringIndexerModel"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        /// <param name="vocabulary">The vocabulary to use</param>
        public StringIndexerModel(string uid, List<string> vocabulary)
            : this(SparkEnvironment.JvmBridge.CallConstructor(
                s_stringIndexerModelClassName, uid, vocabulary))
        {
        }

        internal StringIndexerModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Loads the <see cref="StringIndexerModel"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">
        /// The path the previous <see cref="StringIndexerModel"/> was saved to
        /// </param>
        /// <returns>New <see cref="StringIndexerModel"/> object</returns>
        public static StringIndexerModel Load(string path) =>
            WrapAsStringIndexerModel(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_stringIndexerModelClassName, "load", path));

        /// <summary>
        /// Check transform validity and derive the output schema from the input schema.
        /// 
        /// This checks for validity of interactions between parameters during Transform and
        /// raises an exception if any parameter value is invalid.
        ///
        /// Typical implementation should first conduct verification on schema change and parameter
        /// validity, including complex parameter interaction checks.
        /// </summary>
        /// <param name="value">
        /// The <see cref="StructType"/> of the <see cref="DataFrame"/> which will be transformed.
        /// </param>
        /// <returns>
        /// The <see cref="StructType"/> of the output schema that would have been derived from the
        /// input schema, if Transform had been called.
        /// </returns>
        public StructType TransformSchema(StructType value) =>
            new StructType(
                (JvmObjectReference)_jvmObject.Invoke(
                    "transformSchema",
                    DataType.FromJson(_jvmObject.Jvm, value.Json)));

        /// <summary>
        /// Converts a DataFrame with a text document to a sparse vector of token counts.
        /// </summary>
        /// <param name="document"><see cref="DataFrame"/> to transform</param>
        /// <returns><see cref="DataFrame"/> containing the original data and the counts</returns>
        public DataFrame Transform(DataFrame document) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", document));

        private static StringIndexerModel WrapAsStringIndexerModel(object obj) =>
            new StringIndexerModel((JvmObjectReference)obj);
    }
}
