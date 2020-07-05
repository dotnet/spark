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
    /// <typeparam name="T">
    /// The class that implements FeatureBase, this is needed so we can create new objects where
    /// spark returns new objects rather than update existing objects.
    /// </typeparam>
    public class FeatureBase<T> : Identifiable
    {
        internal readonly JvmObjectReference _jvmObject;
        
        internal FeatureBase(string className)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className), className)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(className);
        }
        
        internal FeatureBase(string className, string uid)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className, uid), className)
        {
            _jvmObject = SparkEnvironment.JvmBridge.CallConstructor(className, uid);
        }
        
        internal FeatureBase(JvmObjectReference jvmObject, string className)
        {
            _jvmObject = jvmObject;
        }

        /// <summary>
        /// Returns the JVM toString value rather than the .NET ToString default
        /// </summary>
        /// <returns>JVM toString() value</returns>
        public override string ToString() => (string)_jvmObject.Invoke("toString");
        
        /// <summary>
        /// The UID that was used to create the object. If no UID is passed in when creating the
        /// object then a random UID is created when the object is created.
        /// </summary>
        /// <returns>string UID identifying the object</returns>
        public string Uid() => (string)_jvmObject.Invoke("uid");

        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        /// <returns>New object</returns>
        public T Save(string path) => 
            WrapAsType((JvmObjectReference)_jvmObject.Invoke("save", path));

        private T WrapAsType(JvmObjectReference reference)
        {
            var constructor = typeof(T)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .Single(p =>
                    p.GetParameters()
                        .All(p => p.ParameterType == typeof(JvmObjectReference)));

            return (T)constructor.Invoke(new object[] {reference});
        }
    }
}
