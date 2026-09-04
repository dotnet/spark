// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;
using static Microsoft.Spark.Utils.UdfUtils;

namespace Microsoft.Spark.Worker.Processor
{
    internal sealed class CommandProcessor
    {
        private const int MaxSpark40Udfs = 1024;
        private const int MaxSpark40ArgumentsPerUdf = 10;
        private const int MaxSpark40Arguments = 10240;
        private const int MaxSpark40ChainedFunctionsPerUdf = 64;
        private const int MaxSpark40ChainedFunctions = 4096;
        private const int MaxSpark40CommandBytes = 16 * 1024 * 1024;
        private const int MaxSpark40TotalCommandBytes = 64 * 1024 * 1024;
        private const int MaxSpark40NameBytes = 4096;

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
            PythonEvalType evalType = ReadEvalType(stream);

            var commandPayload = new CommandPayload()
            {
                EvalType = evalType
            };

            if (evalType == PythonEvalType.NON_UDF)
            {
                commandPayload.Commands = new[] { ReadNonUdfCommand(stream) };
            }
            else
            {
                commandPayload.Commands = ReadSqlCommands(evalType, stream, _version);
            }

            return commandPayload;
        }

        internal PythonEvalType ReadEvalType(Stream stream)
        {
            if ((_version.Major, _version.Minor) != (4, 0))
            {
                return (PythonEvalType)SerDe.ReadInt32(stream);
            }

            byte[] buffer = new byte[sizeof(int)];
            int totalBytesRead = 0;
            while (totalBytesRead < buffer.Length)
            {
                int bytesRead = stream.Read(
                    buffer,
                    totalBytesRead,
                    buffer.Length - totalBytesRead);
                if (bytesRead == 0)
                {
                    throw new EndOfStreamException(
                        "Incomplete Spark 4 evaluation type.");
                }

                totalBytesRead += bytesRead;
            }

            int rawEvalType = BinaryPrimitives.ReadInt32BigEndian(buffer);
            if (rawEvalType == (int)PythonEvalType.SQL_BATCHED_UDF)
            {
                return PythonEvalType.SQL_BATCHED_UDF;
            }

            bool isKnownUnsupported = rawEvalType == 0 ||
                rawEvalType == 101 ||
                (rawEvalType >= 200 && rawEvalType <= 212) ||
                rawEvalType == 300 ||
                rawEvalType == 301;
            if (isKnownUnsupported)
            {
                throw new NotSupportedException(
                    $"Spark 4 evaluation type {rawEvalType} is not supported.");
            }

            throw new InvalidDataException(
                $"Unknown Spark 4 evaluation type: {rawEvalType}.");
        }

        /// <summary>
        /// Read one a non-UDF command from the stream.
        /// Supports both RDD commands and Raw commands.
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <returns>CommandBase object (either RDDCommand or RawCommand)</returns>
        private static CommandBase ReadNonUdfCommand(Stream stream)
        {
            int commandBytesCount = SerDe.ReadInt32(stream);
            if (commandBytesCount <= 0)
            {
                throw new InvalidDataException(
                    $"Invalid command size: {commandBytesCount}");
            }

            object obj = CommandSerDe.DeserializeNonUdf(
                stream,
                out CommandSerDe.SerializedMode serializerMode,
                out CommandSerDe.SerializedMode deserializerMode,
                out var runMode);

            CommandBase command;

            if (obj is RDD.WorkerFunction.ExecuteDelegate rddWorkerFunctionDelegate)
            {
                command = new RDDCommand
                {
                    WorkerFunction = new RDD.WorkerFunction(rddWorkerFunctionDelegate)
                };
            }
            else
            {
                // Raw UDF - provides direct stream access for high-performance scenarios
                command = new RawCommand
                {
                    WorkerFunction = new RawWorkerFunction(
                        (RawWorkerFunction.ExecuteDelegate)obj)
                };
            }

            command.SerializerMode = serializerMode;
            command.DeserializerMode = deserializerMode;

            return command;
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
                (4, 0) => ReadSpark40SqlCommands(evalType, stream),
                _ => throw new NotSupportedException($"Spark {version} not supported.")
            };
        }

        private static SqlCommand[] ReadSpark40SqlCommands(
            PythonEvalType evalType,
            Stream stream)
        {
            if (evalType != PythonEvalType.SQL_BATCHED_UDF)
            {
                throw new NotSupportedException(
                    $"Spark 4 evaluation type {evalType} is not supported.");
            }

            var reader = new ProtocolReader(stream);
            bool isProfiling = reader.ReadBoolean("Spark 4 profiling flag");
            if (isProfiling)
            {
                string profilerName = reader.ReadUtf8(
                    "Spark 4 profiler name",
                    minimumLength: 4,
                    maximumLength: 6);
                if (profilerName != "perf" && profilerName != "memory")
                {
                    throw new InvalidDataException("Invalid Spark 4 profiler name.");
                }
            }

            int numUdfs = reader.ReadInt32("Spark 4 UDF count");
            ValidateRange(numUdfs, 1, MaxSpark40Udfs, "Spark 4 UDF count");

            var frames = new Spark40UdfFrame[numUdfs];
            bool hasNamedArguments = false;
            int totalArguments = 0;
            int totalChainedFunctions = 0;
            int totalCommandBytes = 0;
            int nextOffset = 0;

            for (int udfIndex = 0; udfIndex < numUdfs; ++udfIndex)
            {
                int numArguments = reader.ReadInt32("Spark 4 UDF argument count");
                ValidateRange(
                    numArguments,
                    0,
                    MaxSpark40ArgumentsPerUdf,
                    "Spark 4 UDF argument count");
                totalArguments = AddWithLimit(
                    totalArguments,
                    numArguments,
                    MaxSpark40Arguments,
                    "Spark 4 total argument count");

                var argOffsets = new int[numArguments];
                for (int argIndex = 0; argIndex < numArguments; ++argIndex)
                {
                    int offset = reader.ReadInt32("Spark 4 UDF argument offset");
                    if (offset < 0 || offset > nextOffset)
                    {
                        throw new InvalidDataException(
                            "Invalid Spark 4 UDF argument offset.");
                    }

                    if (offset == nextOffset)
                    {
                        ++nextOffset;
                    }

                    argOffsets[argIndex] = offset;

                    bool hasName = reader.ReadBoolean(
                        "Spark 4 named argument flag");
                    if (hasName)
                    {
                        _ = reader.ReadUtf8(
                            "Spark 4 argument name",
                            minimumLength: 1,
                            maximumLength: MaxSpark40NameBytes);
                        hasNamedArguments = true;
                    }
                }

                int numChainedFunctions = reader.ReadInt32(
                    "Spark 4 chained function count");
                ValidateRange(
                    numChainedFunctions,
                    1,
                    MaxSpark40ChainedFunctionsPerUdf,
                    "Spark 4 chained function count");
                totalChainedFunctions = AddWithLimit(
                    totalChainedFunctions,
                    numChainedFunctions,
                    MaxSpark40ChainedFunctions,
                    "Spark 4 total chained function count");

                var commandBytes = new byte[numChainedFunctions][];
                for (int functionIndex = 0;
                    functionIndex < numChainedFunctions;
                    ++functionIndex)
                {
                    int length = reader.ReadInt32("Spark 4 command length");
                    ValidateRange(
                        length,
                        1,
                        MaxSpark40CommandBytes,
                        "Spark 4 command length");
                    totalCommandBytes = AddWithLimit(
                        totalCommandBytes,
                        length,
                        MaxSpark40TotalCommandBytes,
                        "Spark 4 total command bytes");
                    commandBytes[functionIndex] = reader.ReadBytes(
                        length,
                        "Spark 4 command");
                }

                if (isProfiling)
                {
                    _ = reader.ReadInt64("Spark 4 profiler result ID");
                }

                frames[udfIndex] = new Spark40UdfFrame(
                    argOffsets,
                    commandBytes);
            }

            if (isProfiling)
            {
                throw new NotSupportedException(
                    "Spark 4 UDF profiling is not supported.");
            }

            if (hasNamedArguments)
            {
                throw new NotSupportedException(
                    "Spark 4 named UDF arguments are not supported.");
            }

            foreach (Spark40UdfFrame frame in frames)
            {
                for (int functionIndex = 0;
                    functionIndex < frame.CommandBytes.Length;
                    ++functionIndex)
                {
                    int expectedArity = functionIndex == 0 ?
                        frame.ArgOffsets.Length :
                        1;
                    CommandSerDe.PreflightSpark40(
                        frame.CommandBytes[functionIndex],
                        expectedArity);
                }
            }

            var commands = new SqlCommand[frames.Length];
            for (int udfIndex = 0; udfIndex < frames.Length; ++udfIndex)
            {
                Spark40UdfFrame frame = frames[udfIndex];
                var command = new SqlCommand
                {
                    ArgOffsets = frame.ArgOffsets,
                    NumChainedFunctions = frame.CommandBytes.Length
                };

                for (int functionIndex = 0;
                    functionIndex < frame.CommandBytes.Length;
                    ++functionIndex)
                {
                    int expectedArity = functionIndex == 0 ?
                        frame.ArgOffsets.Length :
                        1;
                    var currentWorkerFunction = new PicklingWorkerFunction(
                        CommandSerDe.DeserializeSpark40<
                            PicklingWorkerFunction.ExecuteDelegate>(
                            frame.CommandBytes[functionIndex],
                            expectedArity,
                            out CommandSerDe.SerializedMode serializerMode,
                            out CommandSerDe.SerializedMode deserializerMode));

                    command.WorkerFunction = command.WorkerFunction == null ?
                        currentWorkerFunction :
                        PicklingWorkerFunction.Chain(
                            (PicklingWorkerFunction)command.WorkerFunction,
                            currentWorkerFunction);
                    command.SerializerMode = serializerMode;
                    command.DeserializerMode = deserializerMode;
                }

                commands[udfIndex] = command;
            }

            return commands;
        }

        private static void ValidateRange(
            int value,
            int minimum,
            int maximum,
            string fieldName)
        {
            if (value < minimum || value > maximum)
            {
                throw new InvalidDataException($"Invalid {fieldName}.");
            }
        }

        private static int AddWithLimit(
            int current,
            int value,
            int maximum,
            string fieldName)
        {
            int total;
            try
            {
                total = checked(current + value);
            }
            catch (OverflowException ex)
            {
                throw new InvalidDataException($"Invalid {fieldName}.", ex);
            }

            if (total > maximum)
            {
                throw new InvalidDataException($"Invalid {fieldName}.");
            }

            return total;
        }

        private sealed class Spark40UdfFrame
        {
            internal Spark40UdfFrame(int[] argOffsets, byte[][] commandBytes)
            {
                ArgOffsets = argOffsets;
                CommandBytes = commandBytes;
            }

            internal int[] ArgOffsets { get; }

            internal byte[][] CommandBytes { get; }
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
