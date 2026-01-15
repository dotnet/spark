// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using static Microsoft.Spark.Utils.CommandSerDe;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// RawWorkerFunction provides direct access to input/output streams for UDF execution.
    /// This enables high-performance scenarios where the user needs direct control over
    /// serialization/deserialization, bypassing the standard row-by-row processing.
    ///
    /// Use cases:
    /// - Custom binary protocols for efficient data transfer
    /// - Streaming large data without intermediate object allocation
    /// - Integration with external serialization libraries
    /// </summary>
    internal sealed class RawWorkerFunction
    {
        /// <summary>
        /// Delegate for raw UDF execution with direct stream access.
        /// </summary>
        /// <param name="splitId">The partition/split index being processed</param>
        /// <param name="inputStream">Raw input stream from Spark</param>
        /// <param name="outputStream">Raw output stream to Spark</param>
        /// <param name="serializedMode">Mode for serializing output data</param>
        /// <param name="deserializedMode">Mode for deserializing input data</param>
        /// <returns>Number of entries processed</returns>
        internal delegate int ExecuteDelegate(
            int splitId,
            Stream inputStream,
            Stream outputStream,
            SerializedMode serializedMode,
            SerializedMode deserializedMode);

        public RawWorkerFunction(ExecuteDelegate func)
        {
            Func = func;
        }

        internal ExecuteDelegate Func { get; }
    }
}
