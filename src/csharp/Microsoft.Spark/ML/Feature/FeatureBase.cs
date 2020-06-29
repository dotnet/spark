using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    public class FeatureBase<T> : Identifiable
    {
        internal readonly JvmObjectReference _jvmObject;
        
        public FeatureBase(string className)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(className);
        }
        
        public FeatureBase(string className, string uid)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(className, uid);
        }
        
        internal FeatureBase(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        //Identifiable
        public override string ToString()
        {
            return (string)_jvmObject.Invoke("toString");
        }
        
        /// <summary>
        /// The uid that was used to create the <see cref="Bucketizer"/>. If no UID is passed in
        /// when creating the <see cref="Bucketizer"/> then a random UID is created when the
        /// <see cref="Bucketizer"/> is created.
        /// </summary>
        /// <returns>string UID identifying the <see cref="Bucketizer"/></returns>
        public string Uid()
        {
            return (string)_jvmObject.Invoke("uid");
        }

        public T Clear(Param.Param param)
        {
            return (T)_jvmObject.Invoke("clear", param);
        }

        public string ExplainParam(Param.Param param)
        {
            return (string)_jvmObject.Invoke("explainParam", param);
        }
        
        public string ExplainParams()
        {
            return (string)_jvmObject.Invoke("explainParams");
        }

        public object Get(Param.Param param)
        {
            return _jvmObject.Invoke("get", param);
        }

        public Param.Param GetParam(string paramName)
        {
            return new Param.Param((JvmObjectReference)_jvmObject.Invoke("getPram", paramName));
        }

        public T Set(Param.Param param, object value)
        {
            return (T)_jvmObject.Invoke("set", param, value);
        }

    }
}
