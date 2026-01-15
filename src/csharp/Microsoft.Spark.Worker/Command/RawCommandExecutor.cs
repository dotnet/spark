// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.Spark.Utils;
using static Microsoft.Spark.Utils.CommandSerDe;

namespace Microsoft.Spark.Worker.Command
{
    /// <summary>
    /// RawCommandExecutor executes raw UDF commands that operate directly on streams.
    ///
    /// Raw UDFs bypass the standard row-by-row serialization/deserialization,
    /// giving the UDF direct access to input and output streams. This enables:
    /// - High-performance data processing with custom serialization
    /// - Streaming large datasets without intermediate object allocation
    /// - Integration with external binary protocols
    /// </summary>
    internal class RawCommandExecutor
    {
        /// <summary>
        /// Executes a raw command by invoking the UDF with direct stream access.
        /// </summary>
        /// <param name="inputStream">Input stream containing data from Spark</param>
        /// <param name="outputStream">Output stream to write results back to Spark</param>
        /// <param name="splitIndex">The partition/split index being processed</param>
        /// <param name="command">The raw command containing the UDF to execute</param>
        /// <returns>Statistics about the execution</returns>
        internal CommandExecutorStat Execute(
            Stream inputStream,
            Stream outputStream,
            int splitIndex,
            RawCommand command)
        {
            var stat = new CommandExecutorStat();

            SerializedMode serializerMode = command.SerializerMode;
            SerializedMode deserializerMode = command.DeserializerMode;
            RawWorkerFunction.ExecuteDelegate func = command.WorkerFunction.Func;

            stat.NumEntriesProcessed =
                func(splitIndex, inputStream, outputStream, serializerMode, deserializerMode);

            return stat;
        }
    }
}
