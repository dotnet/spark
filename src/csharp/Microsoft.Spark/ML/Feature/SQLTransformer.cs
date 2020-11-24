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
    /// A <see cref="SQLTransformer"/> implements the transformations which are defined by SQL statement.
    /// </summary>
    public class SQLTransformer : FeatureBase<SQLTransformer>, IJvmObjectReferenceProvider
    {
        private static readonly string s_sqlTransformerClassName =
    "org.apache.spark.ml.feature.SQLTransformer";

        /// <summary>
        /// Create a <see cref="SQLTransformer"/> without any parameters.
        /// </summary>
        public SQLTransformer() : base(s_sqlTransformerClassName)
        {
        }

        /// <summary>
        /// Create a <see cref="SQLTransformer"/> with a UID that is used to give the
        /// <see cref="SQLTransformer"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public SQLTransformer(string uid) : base(s_sqlTransformerClassName, uid)
        {
        }

        internal SQLTransformer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Executes the <see cref="SQLTransformer"/> and transforms the DataFrame to include the new
        /// column.
        /// </summary>
        /// <param name="source">The DataFrame to transform</param>
        /// <returns>
        /// New <see cref="DataFrame"/> object with the source <see cref="DataFrame"/> transformed
        /// </returns>
        public DataFrame Transform(DataFrame source) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));

        /// <summary>
        /// Executes the <see cref="SQLTransformer"/> and transforms the schema.
        /// </summary>
        /// <param name="value">The Schema to be transformed</param>
        /// <returns>
        /// New <see cref="StructType"/> object with the schema <see cref="StructType"/> transformed
        /// </returns>
        public StructType TransformSchema(StructType value) =>
            new StructType(
                (JvmObjectReference)_jvmObject.Invoke(
                    "transformSchema",
                    DataType.FromJson(_jvmObject.Jvm, value.Json)));

        /// <summary>
        /// Gets the statement.
        /// </summary>
        /// <returns>Statement</returns>
        public string GetStatement() => (string)(_jvmObject.Invoke("getStatement"));

        /// <summary>
        /// Sets the statement to <see cref="SQLTransformer"/>.
        /// </summary>
        /// <param name="statement">SQL Statement</param>
        /// <returns>
        /// <see cref="SQLTransformer"/> with the statement set
        /// </returns>
        public SQLTransformer SetStatement(string statement) =>
            WrapAsSQLTransformer ((JvmObjectReference)_jvmObject.Invoke("setStatement", statement));

        /// <summary>
        /// Loads the <see cref="SQLTransformer"/> that was previously saved using Save.
        /// </summary>
        /// <param name="path">The path the previous <see cref="SQLTransformer"/> was saved to</param>
        /// <returns>New <see cref="SQLTransformer"/> object, loaded from path</returns>
        public static SQLTransformer Load(string path)
        {
            return WrapAsSQLTransformer(
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_sqlTransformerClassName, "load", path));
        }

        private static SQLTransformer WrapAsSQLTransformer(object obj) =>
            new SQLTransformer((JvmObjectReference)obj);
    }
}
