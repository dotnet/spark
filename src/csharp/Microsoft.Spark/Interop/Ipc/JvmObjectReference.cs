// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// JvmObjectId represents the unique owner for a JVM object.
    /// The reason for having another layer on top of string id is
    /// so that JvmObjectReference can be copied.
    /// </summary>
    internal sealed class JvmObjectId
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(JvmObjectId));

        /// <summary>
        /// Constructor for JvmObjectId class.
        /// </summary>
        /// <param name="id">Unique identifier</param>
        /// <param name="jvm">JVM bridge object</param>
        internal JvmObjectId(string id, IJvmBridge jvm)
        {
            Id = id;
            Jvm = jvm;
        }

        ~JvmObjectId()
        {
            if (!Environment.HasShutdownStarted && (Id != null) && (Jvm != null))
            {
                ThreadPool.QueueUserWorkItem(_ => RemoveId());
            }
        }

        /// <summary>
        /// An unique identifier for an object created on the JVM.
        /// </summary>
        internal string Id { get; }

        /// <summary>
        /// JVM bridge object.
        /// </summary>
        internal IJvmBridge Jvm { get; }

        /// <summary>
        /// Implicit conversion to string.
        /// </summary>
        /// <param name="jvmObjectId">JvmObjectId to convert from</param>
        public static implicit operator string(JvmObjectId jvmObjectId) => jvmObjectId.Id;

        /// <summary>
        /// Returns the string version of this object which is the unique id
        /// of the JVM object.
        /// </summary>
        /// <returns>Id of the JVM object</returns>
        public override string ToString() => Id;

        /// <summary>
        /// Checks if the given object is same as the current object by comparing the id.
        /// </summary>
        /// <param name="obj">Other object to compare against</param>
        /// <returns>True if the other object is equal.</returns>
        public override bool Equals(object obj) =>
            ReferenceEquals(this, obj) ||
            ((obj is JvmObjectId jvmObjectId) && Id.Equals(jvmObjectId.Id));

        /// <summary>
        /// Returns the hash code of the current object.
        /// </summary>
        /// <returns>The hash code of the current object</returns>
        public override int GetHashCode() => Id.GetHashCode();

        private void RemoveId()
        {
            Debug.Assert(Id != null);
            Debug.Assert(Jvm != null);

            // This function is registered in the finalizer. If the JVM side is launched
            // in a debug mode, it is possible that the JVM process already exited
            // when this function runs, resulting in an Exception thrown. In this case,
            // the exception is swallowed and logged.
            // In the normal case, there will be no broken connection since JVM launches
            // the .NET process and waits for it to exit.
            try
            {
                Jvm.CallStaticJavaMethod("DotnetHandler", "rm", Id);
            }
            catch (Exception e)
            {
                s_logger.LogException(e);
            }
        }
    }

    /// <summary>
    /// Implemented by objects that contain a <see cref="JvmObjectReference"/>.
    /// </summary>
    public interface IJvmObjectReferenceProvider
    {
        /// <summary>
        /// Gets the <see cref="JvmObjectReference"/> wrapped by the object.
        /// </summary>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        JvmObjectReference Reference { get; }
    }

    /// <summary>
    /// Reference to object created in JVM.
    /// </summary>
    /// <remarks>
    /// This is exposed to help users interact with the JVM. It is provided with limited
    /// support and should be used with caution.
    /// </remarks>
    public sealed class JvmObjectReference : IJvmObjectReferenceProvider
    {
        /// <summary>
        /// The time when this reference was created.
        /// </summary>
        private readonly DateTime _creationTime;

        /// <summary>
        /// Constructor for the JvmObjectReference class.
        /// </summary>
        /// <param name="id">Id for the JVM object</param>
        /// <param name="jvm">IJvmBridge instance that created the JVM object</param>
        internal JvmObjectReference(string id, IJvmBridge jvm)
        {
            if (id is null)
            {
                throw new ArgumentNullException("JvmReferenceId cannot be null.");
            }

            Id = new JvmObjectId(id, jvm);

            _creationTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Copy constructor.
        /// </summary>
        /// <param name="other">Other JvmObjectReference object to copy from.</param>
        internal JvmObjectReference(JvmObjectReference other)
        {
            Id = other.Id;
            _creationTime = other._creationTime;
        }

        /// <summary>
        /// An unique identifier for an object created on the JVM.
        /// </summary>
        internal JvmObjectId Id { get; }

        /// <summary>
        /// IJvmBridge instance that created the JVM object.
        /// </summary>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        public IJvmBridge Jvm => Id.Jvm;

        JvmObjectReference IJvmObjectReferenceProvider.Reference => this;

        /// <summary>
        /// Invokes a method on the JVM object that this JvmObjectReference references to.
        /// </summary>
        /// <param name="arg0">Parameter for the method.</param>
        /// <param name="methodName">Method name to invoke</param>
        /// <returns>The returned object of the method call.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        public object Invoke(string methodName, object arg0) =>
            Jvm.CallNonStaticJavaMethod(this, methodName, arg0);

        /// <summary>
        /// Invokes a method on the JVM object that this JvmObjectReference references to.
        /// </summary>
        /// <param name="arg0">First parameter for the method.</param>
        /// <param name="arg1">Second parameter for the method.</param>
        /// <param name="methodName">Method name to invoke</param>
        /// <returns>The returned object of the method call.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        public object Invoke(string methodName, object arg0, object arg1) =>
            Jvm.CallNonStaticJavaMethod(this, methodName, arg0, arg1);

        /// <summary>
        /// Invokes a method on the JVM object that this JvmObjectReference references to.
        /// </summary>
        /// <param name="methodName">Method name to invoke</param>
        /// <param name="args">Parameters for the method</param>
        /// <returns>The returned object of the method call.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        public object Invoke(string methodName, params object[] args)
        {
            return Jvm.CallNonStaticJavaMethod(this, methodName, args);
        }

        /// <summary>
        /// Returns the string version of this object which is the unique id
        /// of the JVM object.
        /// </summary>
        /// <returns>Id of the JVM object</returns>
        public override string ToString()
        {
            return Id.ToString();
        }

        /// <summary>
        /// Checks if the given object is same as the current object by comparing the ids.
        /// </summary>
        /// <param name="obj">Other object to compare against</param>
        /// <returns>True if the other object is equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return
                (obj is IJvmObjectReferenceProvider provider) &&
                Id.Equals(provider.Reference.Id);
        }

        /// <summary>
        /// Returns the hash code of the current object.
        /// </summary>
        /// <returns>The hash code of the current object</returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        /// <summary>
        /// Gets the debug info on the JVM object that the current object refers to.
        /// </summary>
        /// <returns>The debug info of the JVM object</returns>
        public string GetDebugInfo()
        {
            var classObject = (JvmObjectReference)Invoke("getClass");
            var className = (string)classObject.Invoke("getName");

            return $"Java object reference id={Id}, type name={className}, creation time (UTC)={_creationTime:o}";
        }
    }
}
