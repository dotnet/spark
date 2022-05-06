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
    /// Class <see cref="NGram"/> transformer that converts the input array of strings into
    /// an array of n-grams. Null values in the input array are ignored. It returns an array
    /// of n-grams where each n-gram is represented by a space-separated string of words.
    /// </summary>
    public class NGram :
        JavaTransformer,
        IJavaMLWritable,
        IJavaMLReadable<NGram>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.NGram";

        /// <summary>
        /// Create a <see cref="NGram"/> without any parameters.
        /// </summary>
        public NGram() : base(s_className)
        {
        }

        /// <summary>
        /// Create a <see cref="NGram"/> with a UID that is used to give the
        /// <see cref="NGram"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.
        /// </param>
        public NGram(string uid) : base(s_className, uid)
        {
        }

        internal NGram(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Gets the column that the <see cref="NGram"/> should read from.
        /// </summary>
        /// <returns>string, input column</returns>
        public string GetInputCol() => (string)Reference.Invoke("getInputCol");

        /// <summary>
        /// Sets the column that the <see cref="NGram"/> should read from.
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="NGram"/> object</returns>
        public NGram SetInputCol(string value) => WrapAsNGram(Reference.Invoke("setInputCol", value));

        /// <summary>
        /// Gets the output column that the <see cref="NGram"/> writes.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol() => (string)Reference.Invoke("getOutputCol");

        /// <summary>
        /// Sets the output column that the <see cref="NGram"/> writes.
        /// </summary>
        /// <param name="value">The name of the new column</param>
        /// <returns>New <see cref="NGram"/> object</returns>
        public NGram SetOutputCol(string value) => WrapAsNGram(Reference.Invoke("setOutputCol", value));

        /// <summary>
        /// Gets N value for <see cref="NGram"/>.
        /// </summary>
        /// <returns>int, N value</returns>
        public int GetN() => (int)Reference.Invoke("getN");

        /// <summary>
        /// Sets N value for <see cref="NGram"/>.
        /// </summary>
        /// <param name="value">N value</param>
        /// <returns>New <see cref="NGram"/> object</returns>
        public NGram SetN(int value) => WrapAsNGram(Reference.Invoke("setN", value));

        /// <summary>
        /// Executes the <see cref="NGram"/> and transforms the DataFrame to include the new
        /// column.
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns>
        /// New <see cref="DataFrame"/> object with the source <see cref="DataFrame"/> transformed.
        /// </returns>
        public override DataFrame Transform(DataFrame source) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", source));

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
        /// Loads the <see cref="NGram"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">The path the previous <see cref="NGram"/> was saved to</param>
        /// <returns>New <see cref="NGram"/> object, loaded from path</returns>
        public static NGram Load(string path) =>
            WrapAsNGram(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_className,
                    "load",
                    path));

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
        /// <returns>an <see cref="JavaMLReader&lt;NGram&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<NGram> Read() =>
            new JavaMLReader<NGram>((JvmObjectReference)Reference.Invoke("read"));

        private static NGram WrapAsNGram(object obj) => new NGram((JvmObjectReference)obj);
    }
}
