// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Linq;
using System.Reflection;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// Params is used for components that take parameters. This also provides
    /// an internal param map to store parameter values attached to the instance.
    /// An abstract class corresponds to scala's Params trait.
    /// </summary>
    public abstract class Params : Identifiable, IJvmObjectReferenceProvider
    {
        internal Params(string className)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className))
        {
        }

        internal Params(string className, string uid)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className, uid))
        {
        }

        internal Params(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Returns the JVM toString value rather than the .NET ToString default
        /// </summary>
        /// <returns>JVM toString() value</returns>
        public override string ToString() => (string)Reference.Invoke("toString");

        /// <summary>
        /// The UID that was used to create the object. If no UID is passed in when creating the
        /// object then a random UID is created when the object is created.
        /// </summary>
        /// <returns>string UID identifying the object</returns>
        public string Uid() => (string)Reference.Invoke("uid");

        /// <summary>
        /// Returns a description of how a specific <see cref="Param"/> works and is currently set.
        /// </summary>
        /// <param name="param">The <see cref="Param"/> to explain</param>
        /// <returns>Description of the <see cref="Param"/></returns>
        public string ExplainParam(Param.Param param) =>
            (string)Reference.Invoke("explainParam", param);

        /// <summary>
        /// Returns a description of how all of the <see cref="Param"/>'s that apply to this object
        /// work and how they are currently set.
        /// </summary>
        /// <returns>Description of all the applicable <see cref="Param"/>'s</returns>
        public string ExplainParams() => (string)Reference.Invoke("explainParams");

        /// <summary>Checks whether a param is explicitly set.</summary>
        /// <param name="param">The <see cref="Param"/> to be checked.</param>
        /// <returns>bool</returns>
        public bool IsSet(Param.Param param) => (bool)Reference.Invoke("isSet", param);

        /// <summary>Checks whether a param is explicitly set or has a default value.</summary>
        /// <param name="param">The <see cref="Param"/> to be checked.</param>
        /// <returns>bool</returns>
        public bool IsDefined(Param.Param param) => (bool)Reference.Invoke("isDefined", param);

        /// <summary>
        /// Tests whether this instance contains a param with a given name.
        /// </summary>
        /// <param name="paramName">The <see cref="Param"/> to be test.</param>
        /// <returns>bool</returns>
        public bool HasParam(string paramName) => (bool)Reference.Invoke("hasParam", paramName);

        /// <summary>
        /// Retrieves a <see cref="Param"/> so that it can be used to set the value of the
        /// <see cref="Param"/> on the object.
        /// </summary>
        /// <param name="paramName">The name of the <see cref="Param"/> to get.</param>
        /// <returns><see cref="Param"/> that can be used to set the actual value</returns>
        public Param.Param GetParam(string paramName) =>
            new Param.Param((JvmObjectReference)Reference.Invoke("getParam", paramName));

        /// <summary>
        /// Sets the value of a specific <see cref="Param"/>.
        /// </summary>
        /// <param name="param"><see cref="Param"/> to set the value of</param>
        /// <param name="value">The value to use</param>
        /// <returns>The object that contains the newly set <see cref="Param"/></returns>
        public T Set<T>(Param.Param param, object value) =>
            WrapAsType<T>((JvmObjectReference)Reference.Invoke("set", param, value));

        /// <summary>
        /// Clears any value that was previously set for this <see cref="Param"/>. The value is
        /// reset to the default value.
        /// </summary>
        /// <param name="param">The <see cref="Param"/> to set back to its original value</param>
        /// <returns>Object reference that was used to clear the <see cref="Param"/></returns>
        public T Clear<T>(Param.Param param) =>
            WrapAsType<T>((JvmObjectReference)Reference.Invoke("clear", param));

        protected static T WrapAsType<T>(JvmObjectReference reference)
        {
            ConstructorInfo constructor = typeof(T)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .Single(c =>
                {
                    ParameterInfo[] parameters = c.GetParameters();
                    return (parameters.Length == 1) &&
                        (parameters[0].ParameterType == typeof(JvmObjectReference));
                });

            return (T)constructor.Invoke(new object[] { reference });
        }
    }
}
