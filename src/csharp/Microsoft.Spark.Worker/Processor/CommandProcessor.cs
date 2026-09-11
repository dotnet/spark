// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
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
            if (rawEvalType == (int)PythonEvalType.NON_UDF ||
                rawEvalType == (int)PythonEvalType.SQL_BATCHED_UDF ||
                rawEvalType == (int)PythonEvalType.SQL_SCALAR_PANDAS_UDF ||
                rawEvalType == (int)PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)
            {
                return (PythonEvalType)rawEvalType;
            }

            bool isKnownUnsupported = rawEvalType == 101 ||
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
            var reader = new ProtocolReader(stream);
            bool isArrow = evalType != PythonEvalType.SQL_BATCHED_UDF;
            bool grouped = evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF;
            IReadOnlyDictionary<string, string> configuration = isArrow ?
                ReadSpark40ArrowConfiguration(reader) : null;
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
            ValidateRange(numUdfs, 1, grouped ? 1 : MaxSpark40Udfs, "Spark 4 UDF count");

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
                    grouped ? 2 : (isArrow ? 1 : 0),
                    grouped ? MaxSpark40Arguments : MaxSpark40ArgumentsPerUdf,
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
                    if (offset < 0 || (!grouped && offset > nextOffset))
                    {
                        throw new InvalidDataException(
                            "Invalid Spark 4 UDF argument offset.");
                    }

                    if (!grouped && offset == nextOffset)
                    {
                        ++nextOffset;
                    }

                    argOffsets[argIndex] = offset;

                    if (grouped)
                    {
                        continue;
                    }

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

                int[] groupingKeyOffsets = null;
                if (grouped)
                {
                    int metadataLength = argOffsets[0];
                    int keyCount = argOffsets[1];
                    if (metadataLength != numArguments - 1 || keyCount > numArguments - 2)
                    {
                        throw new InvalidDataException("Invalid Spark 4 grouped-map offsets.");
                    }

                    groupingKeyOffsets = new int[keyCount];
                    Array.Copy(argOffsets, 2, groupingKeyOffsets, 0, keyCount);
                    var values = new int[numArguments - 2 - keyCount];
                    Array.Copy(argOffsets, 2 + keyCount, values, 0, values.Length);
                    argOffsets = values;
                }

                int numChainedFunctions = reader.ReadInt32(
                    "Spark 4 chained function count");
                ValidateRange(
                    numChainedFunctions,
                    1,
                    grouped ? 1 : MaxSpark40ChainedFunctionsPerUdf,
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
                    commandBytes)
                {
                    GroupingKeyOffsets = groupingKeyOffsets
                };
            }

            bool hasRepl = false;
            bool mixedRepresentations = false;
            bool? usesDataFrame = null;
            if (isArrow)
            {
                // Validate every bounded command before resolving even the first
                // user type. Malformed commands take precedence over unsupported
                // profiling, named arguments, REPL, or Arrow configurations.
                foreach (Spark40UdfFrame frame in frames)
                {
                    for (int i = 0; i < frame.CommandBytes.Length; ++i)
                    {
                        bool isDataFrame = CommandSerDe.PreflightSpark40Arrow(
                            frame.CommandBytes[i], i == 0 ? frame.ArgOffsets.Length : 1,
                            grouped, out StructType returnSchema, out bool isRepl);
                        hasRepl |= isRepl;
                        mixedRepresentations |= usesDataFrame.HasValue &&
                            usesDataFrame.Value != isDataFrame;
                        usesDataFrame = isDataFrame;
                        frame.IsDataFrame = isDataFrame;
                        frame.ReturnSchema = returnSchema;
                    }
                }
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

            if (hasRepl || mixedRepresentations ||
                (configuration != null && configuration.TryGetValue(
                    "spark.sql.execution.arrow.useLargeVarTypes", out string largeTypes) &&
                    bool.Parse(largeTypes)))
            {
                throw new NotSupportedException(hasRepl ?
                    "Spark 4 REPL commands are not supported." : mixedRepresentations ?
                    "Spark 4 cannot mix Arrow and DataFrame UDF representations." :
                    "Spark 4 Arrow large variable-width types are not supported.");
            }

            foreach (Spark40UdfFrame frame in isArrow ? Array.Empty<Spark40UdfFrame>() : frames)
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
                    GroupingKeyOffsets = frame.GroupingKeyOffsets,
                    ReturnSchema = frame.ReturnSchema,
                    ArrowConfiguration = configuration,
                    NumChainedFunctions = frame.CommandBytes.Length
                };

                for (int functionIndex = 0;
                    functionIndex < frame.CommandBytes.Length;
                    ++functionIndex)
                {
                    int expectedArity = functionIndex == 0 ?
                        frame.ArgOffsets.Length :
                        1;
                    if (isArrow)
                    {
                        WorkerFunction function = DeserializeSpark40ArrowFunction(
                            frame.CommandBytes[functionIndex], expectedArity, grouped,
                            frame.IsDataFrame, out CommandSerDe.SerializedMode arrowSerializer,
                            out CommandSerDe.SerializedMode arrowDeserializer);
                        command.WorkerFunction = command.WorkerFunction == null ? function :
                            frame.IsDataFrame ?
                            DataFrameWorkerFunction.Chain(
                                (DataFrameWorkerFunction)command.WorkerFunction,
                                (DataFrameWorkerFunction)function) :
                            ArrowWorkerFunction.Chain(
                                (ArrowWorkerFunction)command.WorkerFunction,
                                (ArrowWorkerFunction)function);
                        command.SerializerMode = arrowSerializer;
                        command.DeserializerMode = arrowDeserializer;
                        continue;
                    }

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

        private static IReadOnlyDictionary<string, string> ReadSpark40ArrowConfiguration(
            ProtocolReader reader)
        {
            int count = reader.ReadInt32("Spark 4 Arrow configuration count");
            ValidateRange(count, 0, 64, "Spark 4 Arrow configuration count");
            var configuration = new Dictionary<string, string>(StringComparer.Ordinal);
            int totalBytes = 0;
            for (int i = 0; i < count; ++i)
            {
                string key = reader.ReadUtf8("Spark 4 Arrow configuration key", 1, 256);
                string value = reader.ReadUtf8("Spark 4 Arrow configuration value", 0, 4096);
                totalBytes = AddWithLimit(totalBytes,
                    Encoding.UTF8.GetByteCount(key) + Encoding.UTF8.GetByteCount(value),
                    256 * 1024, "Spark 4 Arrow configuration bytes");
                if (configuration.ContainsKey(key))
                {
                    throw new InvalidDataException("Duplicate Spark 4 Arrow configuration key.");
                }

                if ((key == "spark.sql.execution.arrow.useLargeVarTypes" ||
                    key == "spark.sql.execution.pandas.convertToArrowArraySafely" ||
                    key == "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName") &&
                    !bool.TryParse(value, out _))
                {
                    throw new InvalidDataException("Invalid Spark 4 Arrow boolean configuration.");
                }

                configuration.Add(key, value);
            }

            return configuration;
        }

        private static WorkerFunction DeserializeSpark40ArrowFunction(
            byte[] bytes, int arity, bool grouped, bool dataFrame,
            out CommandSerDe.SerializedMode serializer,
            out CommandSerDe.SerializedMode deserializer)
        {
            if (grouped)
            {
                return dataFrame ?
                    new DataFrameGroupedMapWorkerFunction(
                        CommandSerDe.DeserializeSpark40Arrow<DataFrameGroupedMapWorkerFunction.ExecuteDelegate>(
                            bytes, arity, true, out serializer, out deserializer)) :
                    new ArrowGroupedMapWorkerFunction(
                        CommandSerDe.DeserializeSpark40Arrow<ArrowGroupedMapWorkerFunction.ExecuteDelegate>(
                            bytes, arity, true, out serializer, out deserializer));
            }

            return dataFrame ?
                new DataFrameWorkerFunction(
                    CommandSerDe.DeserializeSpark40Arrow<DataFrameWorkerFunction.ExecuteDelegate>(
                        bytes, arity, false, out serializer, out deserializer)) :
                new ArrowWorkerFunction(
                    CommandSerDe.DeserializeSpark40Arrow<ArrowWorkerFunction.ExecuteDelegate>(
                        bytes, arity, false, out serializer, out deserializer));
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

            internal int[] GroupingKeyOffsets { get; set; }

            internal StructType ReturnSchema { get; set; }

            internal bool IsDataFrame { get; set; }
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
