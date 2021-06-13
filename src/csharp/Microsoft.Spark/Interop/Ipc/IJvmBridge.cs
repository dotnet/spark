// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// Interface of the bridge between JVM and CLR.
    /// </summary>
    /// <remarks>
    /// This is exposed to help users interact with the JVM. It is provided with limited
    /// support and should be used with caution.
    /// </remarks>
    public interface IJvmBridge : IDisposable
    {
        // Each method has three overloads: one argument,
        // two arguments, and a params array of arguments.
        // This covers the bulk of the call sites, avoiding
        // params array allocations for the most common uses,
        // which involve one, two, and zero arguments (the latter
        // is covered by the compiler using Array.Empty with
        // the params array overload).

        /// <summary>
        /// Call java class constructor.
        /// </summary>
        /// <param name="className">The class name.</param>
        /// <param name="arg0">Parameter for the constructor.</param>
        /// <returns>The <see cref="JvmObjectReference"/> of the constructed class.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        JvmObjectReference CallConstructor(
            string className,
            object arg0);

        /// <summary>
        /// Call java class constructor.
        /// </summary>
        /// <param name="className">The class name.</param>
        /// <param name="arg0">First parameter for the constructor.</param>
        /// <param name="arg1">Second parameter for the constructor.</param>
        /// <returns>The <see cref="JvmObjectReference"/> of the constructed class.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        JvmObjectReference CallConstructor(
            string className,
            object arg0,
            object arg1);

        /// <summary>
        /// Call java class constructor.
        /// </summary>
        /// <param name="className">The class name.</param>
        /// <param name="args">Parameters for the constructor.</param>
        /// <returns>The <see cref="JvmObjectReference"/> of the constructed class.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        JvmObjectReference CallConstructor(
            string className,
            params object[] args);

        /// <summary>
        /// Call static java method.
        /// </summary>
        /// <param name="className">The class name.</param>
        /// <param name="methodName">Method name to invoke.</param>
        /// <param name="arg0">Parameter for the method.</param>
        /// <returns>The returned object of the method call.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        object CallStaticJavaMethod(
            string className,
            string methodName,
            object arg0);

        /// <summary>
        /// Call static java method.
        /// </summary>
        /// <param name="className">The class name.</param>
        /// <param name="methodName">Method name to invoke.</param>
        /// <param name="arg0">First parameter for the method.</param>
        /// <param name="arg1">Second parameter for the method.</param>
        /// <returns>The returned object of the method call.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        object CallStaticJavaMethod(
            string className,
            string methodName,
            object arg0,
            object arg1);

        /// <summary>
        /// Call static java method.
        /// </summary>
        /// <param name="className">The class name.</param>
        /// <param name="methodName">Method name to invoke.</param>
        /// <param name="args">Parameters for the method.</param>
        /// <returns>The returned object of the method call.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        object CallStaticJavaMethod(
            string className,
            string methodName,
            params object[] args);

        /// <summary>
        /// Invokes a method on the JVM object that <paramref name="jvmObject"/> references to.
        /// </summary>
        /// <param name="jvmObject">
        /// The <see cref="JvmObjectReference"/> on which to invoke the method.
        /// </param>
        /// <param name="methodName">Method name to invoke.</param>
        /// <param name="arg0">Parameter for the method.</param>
        /// <returns>The returned object of the method call.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        object CallNonStaticJavaMethod(
            JvmObjectReference jvmObject,
            string methodName,
            object arg0);

        /// <summary>
        /// Invokes a method on the JVM object that <paramref name="jvmObject"/> references to.
        /// </summary>
        /// <param name="jvmObject">
        /// The <see cref="JvmObjectReference"/> on which to invoke the method.
        /// </param>
        /// <param name="methodName">Method name to invoke.</param>
        /// <param name="arg0">First parameter for the method.</param>
        /// <param name="arg1">Second parameter for the method.</param>
        /// <returns>The returned object of the method call.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        object CallNonStaticJavaMethod(
            JvmObjectReference jvmObject,
            string methodName,
            object arg0,
            object arg1);

        /// <summary>
        /// Invokes a method on the JVM object that <paramref name="jvmObject"/> references to.
        /// </summary>
        /// <param name="jvmObject">
        /// The <see cref="JvmObjectReference"/> on which to invoke the method.
        /// </param>
        /// <param name="methodName">Method name to invoke.</param>
        /// <param name="args">Parameters for the method.</param>
        /// <returns>The returned object of the method call.</returns>
        /// <remarks>
        /// This is exposed to help users interact with the JVM. It is provided with limited
        /// support and should be used with caution.
        /// </remarks>
        object CallNonStaticJavaMethod(
            JvmObjectReference jvmObject,
            string methodName,
            params object[] args);
    }
}
