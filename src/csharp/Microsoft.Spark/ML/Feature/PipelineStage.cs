// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// <see cref="ScalaPipelineStage"/> A stage in a pipeline, either an Estimator or a Transformer.
    /// </summary>
    public abstract class ScalaPipelineStage : Params
    {
        internal ScalaPipelineStage(string className) : base(className)
        {
        }

        internal ScalaPipelineStage(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaPipelineStage(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Check transform validity and derive the output schema from the input schema.
        ///
        /// We check validity for interactions between parameters during transformSchema
        /// and raise an exception if any parameter value is invalid.
        ///
        /// Typical implementation should first conduct verification on schema change and
        /// parameter validity, including complex parameter interaction checks.
        /// </summary>
        /// <param name="schema">
        /// The <see cref="StructType"/> of the <see cref="DataFrame"/> which will be transformed.
        /// </param>
        /// <returns>
        /// The <see cref="StructType"/> of the output schema that would have been derived from the
        /// input schema, if Transform had been called.
        /// </returns>
        public virtual StructType TransformSchema(StructType schema) =>
             new StructType(
                (JvmObjectReference)Reference.Invoke(
                    "transformSchema",
                    DataType.FromJson(Reference.Jvm, schema.Json)));
    }
}
