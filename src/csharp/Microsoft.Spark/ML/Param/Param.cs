// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature.Param
{
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
        
        public Param(Identifiable parent, string name, string doc, bool isValid)
        {
            _jvmObject = 
                SparkEnvironment.JvmBridge.CallConstructor(
                    s_ParamClassName, parent, name, doc, isValid);
        }
        
        public Param(string parent, string name, string doc, bool isValid)
        {
            _jvmObject = 
                SparkEnvironment.JvmBridge.CallConstructor(
                    s_ParamClassName, parent, name, doc, isValid);
        }
        
        internal Param(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }
        
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
        
        public string Doc()
        {
            return (string)_jvmObject.Invoke("doc");
        }
        
        public object JsonDecode(string json)
        {
            return _jvmObject.Invoke("jsonDecode", json);
        }
        
        public string JsonEncode(object value)
        {
            return (string)_jvmObject.Invoke("jsonEncode", value);
        }
        
        public string Name()
        {
            return (string)_jvmObject.Invoke("name");
        }
        
        public string Parent()
        {
            return (string)_jvmObject.Invoke("parent");
        }
        
    }
}
