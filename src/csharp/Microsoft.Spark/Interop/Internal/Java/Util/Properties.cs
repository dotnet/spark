// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Interop.Internal.Java.Util
{
    /// <summary>
    /// Properties class represents a <c>java.util.Properties</c> object.
    /// </summary>
    internal class Properties : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        /// <summary>
        /// Create a <c>java.util.Properties</c> JVM object
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        internal Properties(IJvmBridge jvm) =>
            _jvmObject = jvm.CallConstructor("java.util.Properties");

        /// <summary>
        /// Create a <c>java.util.Properties</c> JVM object and populate the entries
        /// using <paramref name="properties"/>
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        /// <param name="properties">Dictionary used to populate the
        /// <c>java.util.Properties</c> JVM object</param>
        internal Properties(IJvmBridge jvm, Dictionary<string, string> properties) : this(jvm)
        {
            if (_jvmObject != null)
            {
                foreach (KeyValuePair<string, string> property in properties)
                {
                    _jvmObject.Invoke(
                        "setProperty",
                        property.Key,
                        property.Value);
                }
            }
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
    }
}
