// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;
using static Microsoft.Spark.Utils.UdfUtils;

namespace Microsoft.Spark.Worker.Processor
{
    internal sealed class CommandProcessor
    {
        private readonly Version _version;

        internal CommandProcessor(Version version)
        {
            _version = version;
        }

        /// <summary>
        /// Reads the given stream to construct a CommandPayload object.
        /// </summary>
        /// <param name="stream">The stream to read from</param>
        /// <returns>CommandPayload object</returns>
        internal CommandPayload Process(Stream stream)
        {
            var evalType = (PythonEvalType)SerDe.ReadInt32(stream);

            var commandPayload = new CommandPayload()
            {
                EvalType = evalType
            };

            if (evalType == PythonEvalType.NON_UDF)
            {
                commandPayload.Commands = new[] { ReadRDDCommand(stream) };
            }
            else
            {
                commandPayload.Commands = ReadSqlCommands(evalType, stream, _version);
            }

            return commandPayload;
        }

        /// <summary>
        /// Read one RDDCommand from the stream.
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <returns>RDDCommand object</returns>
        private static RDDCommand ReadRDDCommand(Stream stream)
        {
            int commandBytesCount = SerDe.ReadInt32(stream);
            if (commandBytesCount <= 0)
            {
                throw new InvalidDataException(
                    $"Invalid command size: {commandBytesCount}");
            }

            var rddCommand = new RDDCommand
            {
                WorkerFunction = new RDD.WorkerFunction(
                    CommandSerDe.Deserialize<RDD.WorkerFunction.ExecuteDelegate>(
                    stream,
                    out CommandSerDe.SerializedMode serializerMode,
                    out CommandSerDe.SerializedMode deserializerMode,
                    out var runMode))
            };

            rddCommand.SerializerMode = serializerMode;
            rddCommand.DeserializerMode = deserializerMode;

            return rddCommand;
        }

        /// <summary>
        /// Read SqlCommands from the stream based on the given version.
        /// </summary>
        /// <param name="evalType">Evaluation type for the current commands</param>
        /// <param name="stream">Stream to read from</param>
        /// <param name="version">Spark version</param>
        /// <returns>SqlCommand objects</returns>
        private static SqlCommand[] ReadSqlCommands(
            PythonEvalType evalType,
            Stream stream,
            Version version)
        {
            if ((evalType != PythonEvalType.SQL_BATCHED_UDF) &&
                (evalType != PythonEvalType.SQL_SCALAR_PANDAS_UDF) &&
                (evalType != PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF))
            {
                throw new NotImplementedException($"{evalType} is not supported.");
            }

            return (version.Major, version.Minor) switch
            {
                (2, 4) => SqlCommandProcessorV2_4_X.Process(evalType, stream),
                (3, _) => SqlCommandProcessorV2_4_X.Process(evalType, stream),
                _ => throw new NotSupportedException($"Spark {version} not supported.")
            };
        }

        /// <summary>
        /// Read SqlCommands from the stream.
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="evalType">Evaluation type for the current commands</param>
        /// <returns>SqlCommand objects</returns>
        private static SqlCommand[] ReadSqlCommands(
            PythonEvalType evalType,
            Stream stream)
        {
            int numUdfs = SerDe.ReadInt32(stream);
            var commands = new SqlCommand[numUdfs];

            for (int i = 0; i < numUdfs; ++i)
            {
                var command = new SqlCommand();

                int numArgsOffsets = SerDe.ReadInt32(stream);
                command.ArgOffsets = new int[numArgsOffsets];
                for (int argIndex = 0; argIndex < numArgsOffsets; ++argIndex)
                {
                    command.ArgOffsets[argIndex] = SerDe.ReadInt32(stream);
                }

                command.NumChainedFunctions = SerDe.ReadInt32(stream);
                for (int funcIndex = 0; funcIndex < command.NumChainedFunctions; ++funcIndex)
                {
                    int commandBytesCount = SerDe.ReadInt32(stream);
                    if (commandBytesCount > 0)
                    {
                        CommandSerDe.SerializedMode serializerMode;
                        CommandSerDe.SerializedMode deserializerMode;
                        if (evalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF)
                        {
                            object obj = CommandSerDe.DeserializeArrowOrDataFrameUdf(
                                stream,
                                out serializerMode,
                                out deserializerMode,
                                out string runMode);
                            if (obj is ArrowWorkerFunction.ExecuteDelegate arrowWorkerFunctionDelegate)
                            {
                                var curWorkerFunction = new ArrowWorkerFunction(arrowWorkerFunctionDelegate);
                                command.WorkerFunction = (command.WorkerFunction == null) ?
                                    curWorkerFunction :
                                    ArrowWorkerFunction.Chain(
                                        (ArrowWorkerFunction)command.WorkerFunction,
                                        curWorkerFunction);
                            }
                            else if (obj is DataFrameWorkerFunction.ExecuteDelegate dataFrameWorkerFunctionDelegate)
                            {
                                var curWorkerFunction = new DataFrameWorkerFunction(dataFrameWorkerFunctionDelegate);
                                command.WorkerFunction = (command.WorkerFunction == null) ?
                                    curWorkerFunction :
                                    DataFrameWorkerFunction.Chain(
                                        (DataFrameWorkerFunction)command.WorkerFunction,
                                        curWorkerFunction);
                            }
                            else
                            {
                                throw new NotSupportedException($"Unknown delegate type: {obj.GetType()}");
                            }
                        }
                        else if (evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)
                        {
                            if ((numUdfs != 1) || (command.WorkerFunction != null))
                            {
                                throw new InvalidDataException(
                                    "Grouped map UDFs do not support combining multiple UDFs");
                            }

                            object obj = CommandSerDe.DeserializeArrowOrDataFrameUdf(
                                stream,
                                out serializerMode,
                                out deserializerMode,
                                out string runMode);
                            if (obj is ArrowGroupedMapWorkerFunction.ExecuteDelegate arrowFunctionDelegate)
                            {
                                command.WorkerFunction = new ArrowGroupedMapWorkerFunction(arrowFunctionDelegate);
                            }
                            else if (obj is DataFrameGroupedMapWorkerFunction.ExecuteDelegate dataFrameDelegate)
                            {
                                command.WorkerFunction = new DataFrameGroupedMapWorkerFunction(dataFrameDelegate);
                            }
                            else
                            {
                                throw new NotSupportedException($"Unknown delegate type: {obj.GetType()}");
                            }
                        }
                        else
                        {
                            var curWorkerFunction = new PicklingWorkerFunction(
                                CommandSerDe.Deserialize<PicklingWorkerFunction.ExecuteDelegate>(
                                    stream,
                                    out serializerMode,
                                    out deserializerMode,
                                    out string runMode));

                            command.WorkerFunction = (command.WorkerFunction == null) ?
                                curWorkerFunction :
                                PicklingWorkerFunction.Chain(
                                    (PicklingWorkerFunction)command.WorkerFunction,
                                    curWorkerFunction);
                        }

                        command.SerializerMode = serializerMode;
                        command.DeserializerMode = deserializerMode;
                    }
                    else
                    {
                        throw new InvalidDataException(
                            $"Invalid command size: {commandBytesCount}");
                    }
                }

                commands[i] = command;
            }

            return commands;
        }

        private static class SqlCommandProcessorV2_4_X
        {
            internal static SqlCommand[] Process(PythonEvalType evalType, Stream stream)
            {
                if (evalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF ||
                    evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF ||
                    evalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF ||
                    evalType == PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF)
                {
                    int numConf = SerDe.ReadInt32(stream);
                    for (int i = 0; i < numConf; ++i)
                    {
                        // Currently this setting is not used.
                        // When Arrow supports timestamp type, "spark.sql.session.timeZone"
                        // can be retrieved from here.
                        SerDe.ReadString(stream);
                        SerDe.ReadString(stream);
                    }
                }

                return ReadSqlCommands(evalType, stream);
            }
        }
    }
}
