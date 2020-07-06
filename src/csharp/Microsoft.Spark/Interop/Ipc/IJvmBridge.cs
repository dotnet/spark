// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// Interface of the bridge between JVM and CLR.
    /// </summary>
    internal interface IJvmBridge : IDisposable
    {
        // Each method has three overloads: one argument,
        // two arguments, and a params array of arguments.
        // This covers the bulk of the call sites, avoiding
        // params array allocations for the most common uses,
        // which involve one, two, and zero arguments (the latter
        // is covered by the compiler using Array.Empty with
        // the params array overload).

        JvmObjectReference CallConstructor(
            string className,
            object arg0);

        JvmObjectReference CallConstructor(
            string className,
            object arg0,
            object arg1);

        JvmObjectReference CallConstructor(
            string className,
            params object[] args);


        object CallStaticJavaMethod(
            string className,
            string methodName,
            object arg0);

        object CallStaticJavaMethod(
            string className,
            string methodName,
            object arg0,
            object arg1);

        object CallStaticJavaMethod(
            string className,
            string methodName,
            params object[] args);


        object CallNonStaticJavaMethod(
            JvmObjectReference objectId,
            string methodName,
            object arg0);

        object CallNonStaticJavaMethod(
            JvmObjectReference objectId,
            string methodName,
            object arg0,
            object arg1);

        object CallNonStaticJavaMethod(
            JvmObjectReference objectId,
            string methodName,
            params object[] args);
        
    }
}
