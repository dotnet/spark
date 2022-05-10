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
    /// <see cref="SQLTransformer"/> implements the transformations which are defined by SQL statement.
    /// </summary>
    public class SQLTransformer :
        JavaTransformer,
        IJavaMLWritable,
        IJavaMLReadable<SQLTransformer>
    {
        private static readonly string s_className =
            "org.apache.spark.ml.feature.SQLTransformer";

        /// <summary>
        /// Create a <see cref="SQLTransformer"/> without any parameters.
        /// </summary>
        public SQLTransformer() : base(s_className)
        {
        }

        /// <summary>
        /// Create a <see cref="SQLTransformer"/> with a UID that is used to give the
        /// <see cref="SQLTransformer"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public SQLTransformer(string uid) : base(s_className, uid)
        {
        }

        internal SQLTransformer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Executes the <see cref="SQLTransformer"/> and transforms the DataFrame to include the new
        /// column.
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns>
        /// New <see cref="DataFrame"/> object with the source <see cref="DataFrame"/> transformed.
        /// </returns>
        public override DataFrame Transform(DataFrame source) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", source));

        /// <summary>
        /// Executes the <see cref="SQLTransformer"/> and transforms the schema.
        /// </summary>
        /// <param name="value">The Schema to be transformed</param>
        /// <returns>
        /// New <see cref="StructType"/> object with the schema <see cref="StructType"/> transformed.
        /// </returns>
        public override StructType TransformSchema(StructType value) =>
            new StructType(
                (JvmObjectReference)Reference.Invoke(
                    "transformSchema",
                    DataType.FromJson(Reference.Jvm, value.Json)));

        /// <summary>
        /// Gets the statement.
        /// </summary>
        /// <returns>Statement</returns>
        public string GetStatement() => (string)Reference.Invoke("getStatement");

        /// <summary>
        /// Sets the statement to <see cref="SQLTransformer"/>.
        /// </summary>
        /// <param name="statement">SQL Statement</param>
        /// <returns>
        /// <see cref="SQLTransformer"/> with the statement set.
        /// </returns>
        public SQLTransformer SetStatement(string statement) =>
            WrapAsSQLTransformer((JvmObjectReference)Reference.Invoke("setStatement", statement));

        /// <summary>
        /// Loads the <see cref="SQLTransformer"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">The path the previous <see cref="SQLTransformer"/> was saved to</param>
        /// <returns>New <see cref="SQLTransformer"/> object, loaded from path</returns>
        public static SQLTransformer Load(string path) =>
            WrapAsSQLTransformer(
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
        /// <returns>an <see cref="JavaMLReader&lt;SQLTransformer&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<SQLTransformer> Read() =>
            new JavaMLReader<SQLTransformer>((JvmObjectReference)Reference.Invoke("read"));

        private static SQLTransformer WrapAsSQLTransformer(object obj) =>
            new SQLTransformer((JvmObjectReference)obj);
    }
}
