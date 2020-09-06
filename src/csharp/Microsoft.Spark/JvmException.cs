// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark
{
    /// <summary>
    /// Contains the message returned from the <see cref="Interop.Ipc.JvmBridge"/> on an error.
    /// </summary>
    public class JvmException : Exception
    {
        public JvmException(string message) 
            : base(message)
        {
        }
    }
}
