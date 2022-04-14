// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature.Param
{
    /// <summary>
    /// A param to value map.
    /// </summary>
    public class ParamMap : IJvmObjectReferenceProvider
    {
        private static readonly string s_ParamMapClassName = "org.apache.spark.ml.param.ParamMap";

        /// <summary>
        /// Creates a new instance of a <see cref="ParamMap"/>
        /// </summary>
        public ParamMap() : this(SparkEnvironment.JvmBridge.CallConstructor(s_ParamMapClassName))
        {
        }

        internal ParamMap(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Puts a (param, value) pair (overwrites if the input param exists).
        /// </summary>
        /// <param name="param">The param to be add</param>
        /// <param name="value">The param value to be add</param>
        public ParamMap Put<T>(Param param, T value) =>
            WrapAsParamMap((JvmObjectReference)Reference.Invoke("put", param, value));

        /// <summary>
        /// Returns the string representation of this ParamMap.
        /// </summary>
        /// <returns>representation as string value.</returns>
        public override string ToString() =>
            (string)Reference.Invoke("toString");

        private static ParamMap WrapAsParamMap(object obj) =>
            new ParamMap((JvmObjectReference)obj);
    }
}
