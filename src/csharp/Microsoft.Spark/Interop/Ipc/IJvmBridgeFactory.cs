// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Net;

namespace Microsoft.Spark.Interop.Ipc
{
    internal interface IJvmBridgeFactory
    {
        IJvmBridge Create(int portNumber);

        IJvmBridge Create(IPAddress ip, int portNumber);
    }
}
