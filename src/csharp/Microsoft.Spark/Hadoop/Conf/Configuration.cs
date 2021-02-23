// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Hadoop.Conf
{
    /// <summary>
    /// Provides access to configuration parameters.
    /// </summary>
    public class Configuration : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal Configuration(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
    }
}
