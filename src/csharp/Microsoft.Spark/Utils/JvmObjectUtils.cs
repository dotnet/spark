// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// JvmObjectUtils provides functions related to JVM objects.
    /// </summary>
    internal static class JvmObjectUtils
    {
        /// <summary>
        /// Create a java.util.Properties JVM object and populate the entries
        /// using <paramref name="properties"/>
        /// </summary>
        /// <param name="properties">Dictionary used to populate the
        /// java.util.Properties JVM object</param>
        /// <returns>java.util.Properties JVM object</returns>
        internal static JvmObjectReference CreateProperties(Dictionary<string, string> properties)
        {
            JvmObjectReference javaProperties =
                SparkEnvironment.JvmBridge.CallConstructor("java.util.Properties");

            if (javaProperties != null)
            {
                foreach (KeyValuePair<string, string> property in properties)
                {
                    javaProperties.Invoke(
                        "setProperty",
                        property.Key,
                        property.Value);
                }
            }

            return javaProperties;
        }
    }
}
