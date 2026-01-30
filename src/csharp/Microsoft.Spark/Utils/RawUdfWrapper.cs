// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using static Microsoft.Spark.Utils.CommandSerDe;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// RawUdfWrapper wraps a raw UDF function that operates directly on streams.
    /// This wrapper is used for serialization/deserialization of raw UDFs that need
    /// direct access to input/output streams for high-performance data processing.
    ///
    /// Unlike standard UDFs that process data row-by-row, raw UDFs receive the entire
    /// input stream and have full control over how data is read and written.
    /// </summary>
    [UdfWrapper]
    internal sealed class RawUdfWrapper
    {
        private readonly Func<int, Stream, Stream, SerializedMode, SerializedMode, int> _func;

        internal RawUdfWrapper(Func<int, Stream, Stream, SerializedMode, SerializedMode, int> func)
        {
            _func = func;
        }

        internal int Execute(
            int pid,
            Stream inputStream,
            Stream outputStream,
            SerializedMode serializedMode,
            SerializedMode deserializedMode)
        {
            return _func(pid, inputStream, outputStream, serializedMode, deserializedMode);
        }
    }
}
