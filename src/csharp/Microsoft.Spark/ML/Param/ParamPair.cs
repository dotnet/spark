// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature.Param
{
    /// <summary>
    /// A param and its value.
    /// </summary>
    public sealed class ParamPair<T> : IJvmObjectReferenceProvider
    {
        private static readonly string s_ParamPairClassName = "org.apache.spark.ml.param.ParamPair";

        /// <summary>
        /// Creates a new instance of a <see cref="ParamPair&lt;T&gt;"/>
        /// </summary>
        public ParamPair(Param param, T value)
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_ParamPairClassName, param, value))
        {
        }

        internal ParamPair(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }
    }
}
