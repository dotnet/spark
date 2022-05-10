// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature.Param
{
    /// <summary>
    /// A <see cref="Param"/> with self-contained documentation and optionally default value.
    ///
    /// A <see cref="Param"/> references an individual parameter that includes documentation, the
    /// name of the parameter and optionally a default value. Params can either be set using the
    /// generic <see cref="Param"/> methods or by using explicit methods. For example
    /// <see cref="Bucketizer"/> has <c>SetHandleInvalid</c> or you can call 
    /// <c>GetParam("handleInvalid")</c>and then <see cref="Bucketizer"/>. Set using the
    /// <see cref="Param"/> and the value you want to use.
    /// </summary>
    public class Param : IJvmObjectReferenceProvider
    {
        private static readonly string s_ParamClassName = 
            "org.apache.spark.ml.param.Param";

        /// <summary>
        /// Creates a new instance of a <see cref="Param"/> which will be attached to the parent
        /// specified. The most likely use case for a <see cref="Param"/> is being read from a 
        /// parent object such as <see cref="Bucketizer"/> rather than independently
        /// <param name="parent">The parent object to assign the <see cref="Param"/> to</param>
        /// <param name="name">The name of this <see cref="Param"/></param>
        /// <param name="doc">The documentation for this <see cref="Param"/></param>
        /// </summary>
        public Param(Identifiable parent, string name, string doc)
            : this(SparkEnvironment.JvmBridge.CallConstructor(
                s_ParamClassName, parent.Uid(), name, doc))
        {
        }
        
        /// <summary>
        /// Creates a new instance of a <see cref="Param"/> which will be attached to the parent 
        /// with the UID specified. The most likely use case for a <see cref="Param"/> is being 
        /// read from a parent object such as <see cref="Bucketizer"/> rather than independently
        /// <param name="parent">
        /// The UID of the  parent object to assign the <see cref="Param"/> to
        /// </param>
        /// <param name="name">The name of this <see cref="Param"/></param>
        /// <param name="doc">The documentation for this <see cref="Param"/></param>
        /// </summary>
        public Param(string parent, string name, string doc)
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_ParamClassName, parent, name, doc))
        {
        }

        internal Param(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// The description of what the <see cref="Param"/> does and how it works including any
        /// defaults and the current value
        /// </summary>
        /// <returns>A description of how the <see cref="Param"/> works</returns>
        public string Doc => (string)Reference.Invoke("doc");
        
        /// <summary>
        /// The name of the <see cref="Param"/>
        /// </summary>
        /// <returns>The name of the <see cref="Param"/></returns>
        public string Name => (string)Reference.Invoke("name");

        /// <summary>
        /// The object that contains the <see cref="Param"/>
        /// </summary>
        /// <returns>The UID of the parent oject that this <see cref="Param"/> belongs to</returns>
        public string Parent => (string)Reference.Invoke("parent");
    }
}
