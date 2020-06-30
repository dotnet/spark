using System;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// FeatureBase is to share code amongst all of the ML.Feature objects, there are a few
    /// interfaces that the Scala code implements across all of the objects. This should help to
    /// write the extra objects faster.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class FeatureBase<T> : Identifiable where T: new()
    {
        private readonly string _className;
        internal readonly JvmObjectReference _jvmObject;
        
        protected FeatureBase(string className)
        {
            _className = className;
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(className);
        }
        
        protected FeatureBase(string className, string uid)
        {
            _className = className;
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(className, uid);
        }
        
        internal FeatureBase(JvmObjectReference jvmObject, string className)
        {
            _className = className;
            _jvmObject = jvmObject;
        }

        /// <summary>
        /// Returns the JVM toString value rather than the .Net ToString default
        /// </summary>
        /// <returns>JVM toString() value</returns>
        public override string ToString()
        {
            return (string)_jvmObject.Invoke("toString");
        }
        
        /// <summary>
        /// The uid that was used to create the object. If no UID is passed in when creating the
        /// object then a random UID is created when the object  is created.
        /// </summary>
        /// <returns>string UID identifying the object</returns>
        public string Uid()
        {
            return (string)_jvmObject.Invoke("uid");
        }

        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        /// <returns>New object</returns>
        public T Save(string path)
        {
            return WrapAsType((JvmObjectReference)_jvmObject.Invoke("save", path));
        }

        private T WrapAsType(JvmObjectReference reference)
        {
            var constructor = typeof(T)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .FirstOrDefault(p =>
                    p.GetParameters()
                        .All(p => p.ParameterType == typeof(JvmObjectReference)));

            return (T)constructor.Invoke(new object[] {reference});
        }
    }
}
