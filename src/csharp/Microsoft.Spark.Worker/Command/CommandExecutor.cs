// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;

namespace Microsoft.Spark.Worker.Command
{
    /// <summary>
    /// CommandExecutorStat stores statistics information for executing a command payload.
    /// </summary>
    internal sealed class CommandExecutorStat
    {
        /// <summary>
        /// Number of non-null entries received/processed.
        /// </summary>
        internal int NumEntriesProcessed { get; set; }
    }

    /// <summary>
    /// CommandExecutor reads input data from the input stream,
    /// runs commands on them, and writes result to the output stream.
    /// </summary>
    internal sealed class CommandExecutor
    {
        private readonly Version _version;

        internal CommandExecutor(Version version)
        {
            _version = version;
        }

        /// <summary>
        /// Executes the commands on the input data read from input stream
        /// and writes results to the output stream.
        /// </summary>
        /// <param name="inputStream">Input stream to read data from</param>
        /// <param name="outputStream">Output stream to write results to</param>
        /// <param name="splitIndex">Split index for this task</param>
        /// <param name="commandPayload">Contains the commands to execute</param>
        /// <returns>Statistics captured during the Execute() run</returns>
        internal CommandExecutorStat Execute(
            Stream inputStream,
            Stream outputStream,
            int splitIndex,
            CommandPayload commandPayload)
        {
            if (commandPayload.EvalType == Spark.Utils.UdfUtils.PythonEvalType.NON_UDF)
            {
                if (commandPayload.Commands.Length != 1)
                {
                    throw new System.Exception(
                        "Invalid number of commands for RDD: {commandPayload.Commands.Length}");
                }

                return new RDDCommandExecutor().Execute(
                    inputStream,
                    outputStream,
                    splitIndex,
                    (RDDCommand)commandPayload.Commands[0]);
            }

            return SqlCommandExecutor.Execute(
                _version,
                inputStream,
                outputStream,
                commandPayload.EvalType,
                commandPayload.Commands.Cast<SqlCommand>().ToArray());
        }
    }
}
