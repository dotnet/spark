// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Internal.Java.Util
{
    /// <summary>
    /// Hashtable class represents a <c>java.util.Properties</c> object.
    /// </summary>
    internal sealed class Hashtable : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        /// <summary>
        /// Create a <c>java.util.Hashtable</c> JVM object
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        internal Hashtable(IJvmBridge jvm) =>
            _jvmObject = jvm.CallConstructor("java.util.Hashtable");

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Maps the specified key to the specified value in this Hashtable.
        /// Neither the key nor the value can be null.
        /// </summary>
        /// <param name="key">The Hashtable key</param>
        /// <param name="value">The value</param>
        internal void Put(object key, object value) =>
            _jvmObject.Invoke("put", key, value);
    }
}
