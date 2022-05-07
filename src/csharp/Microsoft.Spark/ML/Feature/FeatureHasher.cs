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
    public class FeatureHasher :
        JavaTransformer,
        IJavaMLWritable,
        IJavaMLReadable<FeatureHasher>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.FeatureHasher";

        /// <summary>
        /// Creates a <see cref="FeatureHasher"/> without any parameters.
        /// </summary>
        public FeatureHasher() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="FeatureHasher"/> with a UID that is used to give the
        /// <see cref="FeatureHasher"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public FeatureHasher(string uid) : base(s_className, uid)
        {
        }

        internal FeatureHasher(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Loads the <see cref="FeatureHasher"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">
        /// The path the previous <see cref="FeatureHasher"/> was saved to.
        /// </param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public static FeatureHasher Load(string path) =>
            WrapAsFeatureHasher(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_className,
                    "load",
                    path));

        /// <summary>
        /// Gets a list of the columns which have been specified as categorical columns.
        /// </summary>
        /// <returns>List of categorical columns, set by SetCategoricalCols</returns>
        public IEnumerable<string> GetCategoricalCols() =>
            (string[])Reference.Invoke("getCategoricalCols");

        /// <summary>
        /// Marks columns as categorical columns.
        /// </summary>
        /// <param name="value">List of column names to mark as categorical columns</param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public FeatureHasher SetCategoricalCols(IEnumerable<string> value) =>
            WrapAsFeatureHasher(Reference.Invoke("setCategoricalCols", value));

        /// <summary>
        /// Gets the columns that the <see cref="FeatureHasher"/> should read from and convert into
        /// hashes. This would have been set by SetInputCol.
        /// </summary>
        /// <returns>List of the input columns, set by SetInputCols</returns>
        public IEnumerable<string> GetInputCols() => (string[])Reference.Invoke("getInputCols");

        /// <summary>
        /// Sets the columns that the <see cref="FeatureHasher"/> should read from and convert into
        /// hashes.
        /// </summary>
        /// <param name="value">The name of the column to as use the source of the hash</param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public FeatureHasher SetInputCols(IEnumerable<string> value) =>
            WrapAsFeatureHasher(Reference.Invoke("setInputCols", value));

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
        /// <param name="value">int value of number of features</param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public FeatureHasher SetNumFeatures(int value) =>
            WrapAsFeatureHasher(Reference.Invoke("setNumFeatures", value));

        /// <summary>
        /// Gets the name of the column the output data will be written to. This is set by
        /// SetInputCol.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol() => (string)Reference.Invoke("getOutputCol");

        /// <summary>
        /// Sets the name of the new column in the <see cref="DataFrame"/> created by Transform.
        /// </summary>
        /// <param name="value">The name of the new column which will contain the hash</param>
        /// <returns>New <see cref="FeatureHasher"/> object</returns>
        public FeatureHasher SetOutputCol(string value) =>
            WrapAsFeatureHasher(Reference.Invoke("setOutputCol", value));

        /// <summary>
        /// Transforms the input <see cref="DataFrame"/>. It is recommended that you validate that
        /// the transform will succeed by calling TransformSchema.
        /// </summary>
        /// <param name="value">Input <see cref="DataFrame"/> to transform</param>
        /// <returns>Transformed <see cref="DataFrame"/></returns>
        public override DataFrame Transform(DataFrame value) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", value));

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
        public override StructType TransformSchema(StructType value) =>
            new StructType(
                (JvmObjectReference)Reference.Invoke(
                    "transformSchema",
                    DataType.FromJson(Reference.Jvm, value.Json)));

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
        /// <returns>an <see cref="JavaMLReader&lt;FeatureHasher&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<FeatureHasher> Read() =>
            new JavaMLReader<FeatureHasher>((JvmObjectReference)Reference.Invoke("read"));

        private static FeatureHasher WrapAsFeatureHasher(object obj) =>
            new FeatureHasher((JvmObjectReference)obj);
    }
}
