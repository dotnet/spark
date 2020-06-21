// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// Interface for handling callbacks between the JVM and Dotnet.
    /// </summary>
    internal interface ICallbackHandler
    {
        void Run(Stream inputStream, Stream outputStream);
    }
}
