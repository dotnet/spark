// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// <see cref="JavaTransformer"/> Abstract class for transformers that transform one dataset into another.
    /// </summary>
    public abstract class JavaTransformer : JavaPipelineStage
    {
        internal JavaTransformer(string className) : base(className)
        {
        }

        internal JavaTransformer(string className, string uid) : base(className, uid)
        {
        }

        internal JavaTransformer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Executes the transformer and transforms the DataFrame to include new columns.
        /// </summary>
        /// <param name="dataset">The Dataframe to be transformed.</param>
        /// <returns>
        /// <see cref="DataFrame"/> containing the original data and new columns.
        /// </returns>
        public virtual DataFrame Transform(DataFrame dataset) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", dataset));
    }
}
