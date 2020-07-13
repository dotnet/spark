// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
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
    /// <see cref="Bucketizer"/> has SetHandleInvalid or you can call GetParam("handleInvalid")
    /// and then <see cref="Bucketizer"/>.Set using the <see cref="Param"/> and the value you want
    /// to use.
    /// </summary>
    public class Param : IJvmObjectReferenceProvider
    {
        private static readonly string s_ParamClassName = 
            "org.apache.spark.ml.param.Param";
        
        private readonly JvmObjectReference _jvmObject;
        
        public Param(Identifiable parent, string name, string doc)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                s_ParamClassName, parent.Uid(), name, doc);
        }
        
        public Param(string parent, string name, string doc)
        {
            _jvmObject = 
                SparkEnvironment.JvmBridge.CallConstructor(s_ParamClassName, parent, name, doc);
        }

        internal Param(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }
        
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
        
        /// <summary>
        /// The description of what the <see cref="Param"/> does and how it works including any
        /// defaults and the current value
        /// </summary>
        /// <returns>A description of how the <see cref="Param"/> works</returns>
        public string Doc
        {
            get => (string)_jvmObject.Invoke("doc");
        }
        
        /// <summary>
        /// The name of the <see cref="Param"/>
        /// </summary>
        /// <returns></returns>
        public string Name 
        {
            get => (string)_jvmObject.Invoke("name");  
        }

        /// <summary>
        /// The object that contains the <see cref="Param"/>
        /// </summary>
        /// <returns></returns>
        public string Parent
        {
            get => (string)_jvmObject.Invoke("parent");
        }
    }
}
