// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Interop.Internal.Java.Util
{
    /// <summary>
    /// ArrayList class represents a <c>java.util.ArrayList</c> object.
    /// </summary>
    internal sealed class ArrayList : IJvmObjectReferenceProvider
    {

        /// <summary>
        /// Create a <c>java.util.ArrayList</c> JVM object
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        internal ArrayList(IJvmBridge jvm)
        {
            Reference = jvm.CallConstructor("java.util.ArrayList");
        }

        public JvmObjectReference Reference { get; private set; }

        internal void Add(object element)
        {
            Reference.Invoke("add", element);
        }

        internal void AddAll(IEnumerable<object> collection)
        {
            foreach (object elem in collection)
            {
                Add(elem);
            }
        }
    }
}
