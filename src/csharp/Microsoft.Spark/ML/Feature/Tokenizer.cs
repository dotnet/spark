// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// A <see cref="Tokenizer"/> that converts the input string to lowercase and then splits it by
    /// white spaces.
    /// </summary>
    public class Tokenizer :
        JavaTransformer,
        IJavaMLWritable,
        IJavaMLReadable<Tokenizer>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.Tokenizer";

        /// <summary>
        /// Create a <see cref="Tokenizer"/> without any parameters
        /// </summary>
        public Tokenizer() : base(s_className)
        {
        }

        /// <summary>
        /// Create a <see cref="Tokenizer"/> with a UID that is used to give the
        /// <see cref="Tokenizer"/> a unique ID
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Tokenizer(string uid) : base(s_className, uid)
        {
        }

        internal Tokenizer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Gets the column that the <see cref="Tokenizer"/> should read from
        /// </summary>
        /// <returns>string, input column</returns>
        public string GetInputCol() => (string)(Reference.Invoke("getInputCol"));

        /// <summary>
        /// Sets the column that the <see cref="Tokenizer"/> should read from
        /// </summary>
        /// <param name="value">The name of the column to as the source</param>
        /// <returns>New <see cref="Tokenizer"/> object</returns>
        public Tokenizer SetInputCol(string value) =>
            WrapAsTokenizer(Reference.Invoke("setInputCol", value));

        /// <summary>
        /// The <see cref="Tokenizer"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <returns>string, the output column</returns>
        public string GetOutputCol() => (string)(Reference.Invoke("getOutputCol"));

        /// <summary>
        /// The <see cref="Tokenizer"/> will create a new column in the DataFrame, this is the
        /// name of the new column.
        /// </summary>
        /// <param name="value">The name of the new column</param>
        /// <returns>New <see cref="Tokenizer"/> object</returns>
        public Tokenizer SetOutputCol(string value) =>
            WrapAsTokenizer(Reference.Invoke("setOutputCol", value));

        /// <summary>
        /// Executes the <see cref="Tokenizer"/> and transforms the DataFrame to include the new
        /// column
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns>
        /// New <see cref="DataFrame"/> object with the source <see cref="DataFrame"/> transformed
        /// </returns>
        public override DataFrame Transform(DataFrame source) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", source));

        /// <summary>
        /// Loads the <see cref="Tokenizer"/> that was previously saved using Save
        /// </summary>
        /// <param name="path">The path the previous <see cref="Tokenizer"/> was saved to</param>
        /// <returns>New <see cref="Tokenizer"/> object, loaded from path</returns>
        public static Tokenizer Load(string path)
        {
            return WrapAsTokenizer(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_className, "load", path));
        }

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
        /// <returns>an <see cref="JavaMLReader&lt;Tokenizer&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<Tokenizer> Read() =>
            new JavaMLReader<Tokenizer>((JvmObjectReference)Reference.Invoke("read"));

        private static Tokenizer WrapAsTokenizer(object obj) =>
            new Tokenizer((JvmObjectReference)obj);
    }
}
