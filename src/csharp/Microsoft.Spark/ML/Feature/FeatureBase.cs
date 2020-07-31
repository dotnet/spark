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
            : this(SparkEnvironment.JvmBridge.CallConstructor(className))
        {
        }
        
        internal FeatureBase(string className, string uid)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className, uid))
        {
        }
        
        internal FeatureBase(JvmObjectReference jvmObject)
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

        /// <summary>
        /// Clears any value that was previously set for this <see cref="Param"/>. The value is
        /// reset to the default value.
        /// </summary>
        /// <param name="param">The <see cref="Param"/> to set back to its original value</param>
        /// <returns>Object reference that was used to clear the <see cref="Param"/></returns>
        public T Clear(Param.Param param) => 
            WrapAsType((JvmObjectReference)_jvmObject.Invoke("clear", param));

        /// <summary>
        /// Returns a description of how a specific <see cref="Param"/> works and is currently set.
        /// </summary>
        /// <param name="param">The <see cref="Param"/> to explain</param>
        /// <returns>Description of the <see cref="Param"/></returns>
        public string ExplainParam(Param.Param param) => 
            (string)_jvmObject.Invoke("explainParam", param);

        /// <summary>
        /// Returns a description of how all of the <see cref="Param"/>'s that apply to this object
        /// work and how they are currently set.
        /// </summary>
        /// <returns>Description of all the applicable <see cref="Param"/>'s</returns>
        public string ExplainParams() => (string)_jvmObject.Invoke("explainParams");

        /// <summary>
        /// Retrieves a <see cref="Param"/> so that it can be used to set the value of the
        /// <see cref="Param"/> on the object.
        /// </summary>
        /// <param name="paramName">The name of the <see cref="Param"/> to get.</param>
        /// <returns><see cref="Param"/> that can be used to set the actual value</returns>
        public Param.Param GetParam(string paramName) => 
            new Param.Param((JvmObjectReference)_jvmObject.Invoke("getParam", paramName));

        /// <summary>
        /// Sets the value of a specific <see cref="Param"/>.
        /// </summary>
        /// <param name="param"><see cref="Param"/> to set the value of</param>
        /// <param name="value">The value to use</param>
        /// <returns>The object that contains the newly set <see cref="Param"/></returns>
        public T Set(Param.Param param, object value) => 
            WrapAsType((JvmObjectReference)_jvmObject.Invoke("set", param, value));

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
