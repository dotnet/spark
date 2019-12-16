using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.ML.Feature
{
    public class Bucketizer : IJvmObjectReferenceProvider
    {
       
        internal Bucketizer(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }
        public Bucketizer()
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor("org.apache.spark.ml.feature.Bucketizer");
        }
        
        public Bucketizer(string uid)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor("org.apache.spark.ml.feature.Bucketizer", uid);
        }

        public static Bucketizer Load(string path)
        {
            return 
                WrapAsBucketizer(
                    SparkEnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.ml.feature.Bucketizer", "load",
                        path));
        } 
        
        public void Save(string path)
        {
                _jvmObject.Invoke("org.apache.spark.ml.feature.Bucketizer", "save",path);
        } 
        
        private readonly JvmObjectReference _jvmObject = null;
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        public Bucketizer SetSplits(double[] value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setSplits", value));
        }

        public double[] GetSplits()
        {
            return (double[])_jvmObject.Invoke("getSplits");
        }

        public string GetInputCol()
        {
            return (string)_jvmObject.Invoke("getInputCol");
        }
        
        public Bucketizer SetInputCol(string value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setInputCol", value));
        }

        public Bucketizer SetOutputCol(string value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setOutputCol", value));
        }

        public DataFrame Transform(DataFrame source)
        {
            return new DataFrame((JvmObjectReference)_jvmObject.Invoke("transform", source));
        }
        private static Bucketizer WrapAsBucketizer(object obj)
        {
            return new Bucketizer((JvmObjectReference)obj);
        }

        public string Uid()
        {
            return (string)_jvmObject.Invoke("uid");
        }

        public string GetHandleInvalid()
        {
            return (string)_jvmObject.Invoke("getHandleInvalid");
        }
        
        public Bucketizer SetHandleInvalid(string value)
        {
            return WrapAsBucketizer(_jvmObject.Invoke("setHandleInvalid", value));
        }

        public StructType TransformSchema(StructType schema)
        {
            return (StructType)_jvmObject.Invoke("transformScherma", schema);
        }
    }
}
