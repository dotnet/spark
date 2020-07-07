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
        
        internal FeatureBase()
            : this(SparkEnvironment.JvmBridge.CallConstructor(ImplementingJavaClassName))
        {
        }
        
        internal FeatureBase(string uid)
            : this(SparkEnvironment.JvmBridge.CallConstructor(ImplementingJavaClassName, uid))
        {
        }
        
        internal FeatureBase(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        protected static string ImplementingJavaClassName;

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
        /// Loads the object that was previously saved using Save
        /// </summary>
        /// <param name="path">The path the previous object was saved to</param>
        /// <returns>New object</returns>
        public static T Load(string path) =>
            WrapAsType((JvmObjectReference)
                SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    ImplementingJavaClassName,"load", path));
        
        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        /// <returns>New object</returns>
        public T Save(string path) => 
            WrapAsType((JvmObjectReference)_jvmObject.Invoke("save", path));

        private static T WrapAsType(JvmObjectReference reference)
        {
            ConstructorInfo constructor = typeof(T)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .Single(c =>
                {
                    ParameterInfo[] parameters = c.GetParameters();
                    return (parameters.Length == 1) &&
                           (parameters[0].ParameterType == typeof(JvmObjectReference));
                });

            return (T)constructor.Invoke(new object[] {reference});
        }
    }
}
