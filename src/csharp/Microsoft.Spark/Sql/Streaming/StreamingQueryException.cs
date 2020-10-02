// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Streaming
{
    /// <summary>
    /// Exception that stopped a <see cref="StreamingQuery"/>.
    /// </summary>
    public class StreamingQueryException : JvmException
    {
        public StreamingQueryException(string message)
            : base(message)
        {
        }
    }
}
