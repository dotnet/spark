using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Newtonsoft.Json;

namespace Microsoft.Spark.ML.Param
{
    /// <summary>
    /// Internal class used to help the `Bucketizer` pass a double[][] into the JVM.
    /// </summary>
    class DoubleArrayArrayParam : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        public DoubleArrayArrayParam(object parent, string name, string doc, double[][] param)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.ml.param.DoubleArrayArrayParam",
                parent, name, doc);

            string json = JsonConvert.SerializeObject(param);
            ReferenceValue = jsonDecode(json);
        }
        
        private JvmObjectReference jsonDecode(string json)
        {
            return (JvmObjectReference)_jvmObject.Invoke("jsonDecode", json);
        }
        public JvmObjectReference Reference { get; }
        
        /// <summary>
        /// This is the JVM version of the double[][] so that it can be used by the `Bucketizer`, to
        /// get the double[][] across the SerDe this serializes as JSON and used jsonDecode on the
        /// JVM side to get a double[][]. ReferenceValue is the double[][].
        /// </summary>
        public JvmObjectReference ReferenceValue { get; }
    }
}
