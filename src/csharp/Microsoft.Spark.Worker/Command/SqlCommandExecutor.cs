// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;
using Razorvine.Pickle;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;

namespace Microsoft.Spark.Worker.Command
{
    /// <summary>
    /// SqlCommandExecutor reads input data from the input stream,
    /// runs commands on them, and writes result to the output stream.
    /// </summary>
    internal abstract class SqlCommandExecutor
    {
        /// <summary>
        /// Executes the commands on the input data read from input stream
        /// and writes results to the output stream.
        /// </summary>
        /// <param name="version">Spark version</param>
        /// <param name="inputStream">Input stream to read data from</param>
        /// <param name="outputStream">Output stream to write results to</param>
        /// <param name="evalType">Evaluation type for the current commands</param>
        /// <param name="commands">Contains the commands to execute</param>
        /// <returns>Statistics captured during the Execute() run</returns>
        internal static CommandExecutorStat Execute(
            Version version,
            Stream inputStream,
            Stream outputStream,
            UdfUtils.PythonEvalType evalType,
            SqlCommand[] commands)
        {
            if (commands.Length <= 0)
            {
                throw new ArgumentException("Commands cannot be empty.");
            }

            if (commands.Any(c =>
                    (c.SerializerMode != CommandSerDe.SerializedMode.Row) ||
                    (c.DeserializerMode != CommandSerDe.SerializedMode.Row)))
            {
                throw new ArgumentException("Unexpected serialization mode found.");
            }

            SqlCommandExecutor executor;
            if (evalType == UdfUtils.PythonEvalType.SQL_BATCHED_UDF)
            {
                executor = new PicklingSqlCommandExecutor();
            }
            else if (evalType == UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF)
            {
                executor = new ArrowOrDataFrameSqlCommandExecutor(version);
            }
            else if (evalType == UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)
            {
                executor = new ArrowOrDataFrameGroupedMapCommandExecutor(version);
            }
            else
            {
                throw new NotSupportedException($"{evalType} is not supported.");
            }

            return executor.ExecuteCore(inputStream, outputStream, commands);
        }

        protected internal abstract CommandExecutorStat ExecuteCore(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands);
    }

    /// <summary>
    /// A SqlCommandExecutor that reads and writes using the
    /// Python pickling format.
    /// </summary>
    internal class PicklingSqlCommandExecutor : SqlCommandExecutor
    {
        [ThreadStatic]
        private static Pickler s_pickler;

        [ThreadStatic]
        private static byte[] s_outputBuffer;

        protected internal override CommandExecutorStat ExecuteCore(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            var stat = new CommandExecutorStat();
            ICommandRunner commandRunner = CreateCommandRunner(commands);

            // On the Spark side, each object in the following List<> is considered as a row.
            // See the ICommandRunner comments above for the types for a row.
            var outputRows = new List<object>();

            // If the input is empty (no rows) or all rows have been read, then
            // SpecialLengths.END_OF_DATA_SECTION is sent as the messageLength.
            // For example, no rows:
            //   +---+----+
            //   |age|name|
            //   +---+----+
            //   +---+----+
            int messageLength;
            while ((messageLength = SerDe.ReadInt32(inputStream)) !=
                (int)SpecialLengths.END_OF_DATA_SECTION)
            {
                if ((messageLength > 0) || (messageLength == (int)SpecialLengths.NULL))
                {
                    if (messageLength <= 0)
                    {
                        throw new InvalidDataException(
                            $"Invalid message length: {messageLength}");
                    }

                    // Each row in inputRows is of type object[]. If a null is present in a row
                    // then the corresponding index column of the row object[] will be set to null.
                    // For example, (inputRows.Length == 2) and (inputRows[0][0] == null)
                    //   +----+
                    //   | age|
                    //   +----+
                    //   |null|
                    //   |  11|
                    //   +----+
                    object[] inputRows =
                        PythonSerDe.GetUnpickledObjects(inputStream, messageLength);

                    for (int i = 0; i < inputRows.Length; ++i)
                    {
                        object row = inputRows[i];
                        // The following can happen if an UDF takes Row object(s).
                        // The JVM Spark side sends a Row object that wraps all the columns used
                        // in the UDF, thus, it is normalized below (the extra layer is removed).
                        if (row is Row r)
                        {
                            row = r.Values;
                        }

                        // Split id is not used for SQL UDFs, so 0 is passed.
                        outputRows.Add(commandRunner.Run(0, row));
                    }

                    // The initial (estimated) buffer size for pickling rows is set to the size of
                    // input pickled rows because the number of rows are the same for both input
                    // and output.
                    WriteOutput(outputStream, outputRows, messageLength);
                    stat.NumEntriesProcessed += inputRows.Length;
                    outputRows.Clear();
                }
            }

            return stat;
        }

        /// <summary>
        /// Writes the given message to the stream.
        /// </summary>
        /// <param name="stream">Stream to write to</param>
        /// <param name="rows">Rows to write to</param>
        /// <param name="sizeHint">
        /// Estimated max size of the serialized output.
        /// If it's not big enough, pickler increases the buffer.
        /// </param>
        private void WriteOutput(Stream stream, IEnumerable<object> rows, int sizeHint)
        {
            if (s_outputBuffer == null)
                s_outputBuffer = new byte[sizeHint];

            Pickler pickler = s_pickler ??= new Pickler(false);
            pickler.dumps(rows, ref s_outputBuffer, out int bytesWritten);

            if (bytesWritten <= 0)
            {
                throw new Exception($"Serialized output size must be positive. Was {bytesWritten}.");
            }

            SerDe.Write(stream, bytesWritten);
            SerDe.Write(stream, s_outputBuffer, bytesWritten);
        }

        /// <summary>
        /// Creates an ICommandRunner instance based on the given commands.
        /// </summary>
        /// <param name="commands">Commands used for creating a command runner</param>
        /// <returns>An ICommandRunner instance</returns>
        private static ICommandRunner CreateCommandRunner(SqlCommand[] commands)
        {
            return (commands.Length == 1) ?
                (ICommandRunner)new SingleCommandRunner(commands[0]) :
                new MultiCommandRunner(commands);
        }

        /// <summary>
        /// Interface for running commands.
        /// On the Spark side, the following is expected for the Pickling to work:
        /// If there is a single command (one UDF), the computed value is returned
        /// as an object (one element). If there are multiple commands (multiple UDF scenario),
        /// the computed value should be an array (not IEnumerable) where each element
        /// in the array corresponds to the value returned by a command.
        /// Refer to EvaluatePython.scala for StructType case.
        /// </summary>
        private interface ICommandRunner
        {
            /// <summary>
            /// Runs commands based on the given split id and input.
            /// </summary>
            /// <param name="splitId">Split id for the commands to run</param>
            /// <param name="input">Input data for the commands to run</param>
            /// <returns>Value returned by running the commands</returns>
            object Run(int splitId, object input);
        }

        /// <summary>
        /// SingleCommandRunner handles running a single command.
        /// </summary>
        private sealed class SingleCommandRunner : ICommandRunner
        {
            /// <summary>
            /// A command to run.
            /// </summary>
            private readonly SqlCommand _command;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="command">A command to run</param>
            internal SingleCommandRunner(SqlCommand command)
            {
                _command = command;
            }

            /// <summary>
            /// Runs a single command.
            /// </summary>
            /// <param name="splitId">Split id for the command to run</param>
            /// <param name="input">Input data for the command to run</param>
            /// <returns>Value returned by running the command</returns>
            public object Run(int splitId, object input)
            {
                return ((PicklingWorkerFunction)_command.WorkerFunction).Func(
                    splitId,
                    (object[])input,
                    _command.ArgOffsets);
            }
        }

        /// <summary>
        /// MultiCommandRunner handles running multiple commands.
        /// </summary>
        private sealed class MultiCommandRunner : ICommandRunner
        {
            /// <summary>
            /// Commands to run.
            /// </summary>
            private readonly SqlCommand[] _commands;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="commands">Multiple commands top run</param>
            internal MultiCommandRunner(SqlCommand[] commands)
            {
                _commands = commands;
            }

            /// <summary>
            /// Runs multiple commands.
            /// </summary>
            /// <param name="splitId">Split id for the commands to run</param>
            /// <param name="input">Input data for the commands to run</param>
            /// <returns>An array of values returned by running the commands</returns>
            public object Run(int splitId, object input)
            {
                var row = new object[_commands.Length];
                for (int i = 0; i < _commands.Length; ++i)
                {
                    SqlCommand command = _commands[i];
                    row[i] = ((PicklingWorkerFunction)command.WorkerFunction).Func(
                        splitId,
                        (object[])input,
                        command.ArgOffsets);
                }

                return row;
            }
        }
    }

    internal abstract class ArrowBasedCommandExecutor : SqlCommandExecutor
    {
        protected Version _version;

        protected IpcOptions ArrowIpcOptions() =>
            new IpcOptions
            {
                WriteLegacyIpcFormat = _version.Major switch
                {
                    2 => true,
                    3 => false,
                    _ => throw new NotSupportedException($"Spark {_version} not supported.")
                }
            };

        protected IEnumerable<RecordBatch> GetInputIterator(Stream inputStream)
        {
            using var reader = new ArrowStreamReader(inputStream, leaveOpen: true);
            RecordBatch batch;
            bool returnedResult = false;
            while ((batch = reader.ReadNextRecordBatch()) != null)
            {
                yield return batch;
                returnedResult = true;
            }

            if (!returnedResult)
            {
                // When no input batches were received, return an empty RecordBatch
                // in order to create and write back the result schema.

                int columnCount = reader.Schema.FieldsList.Count;
                var arrays = new IArrowArray[columnCount];
                for (int i = 0; i < columnCount; ++i)
                {
                    IArrowType type = reader.Schema.GetFieldByIndex(i).DataType;
                    arrays[i] = ArrowArrayHelpers.CreateEmptyArray(type);
                }

                yield return new RecordBatch(reader.Schema, arrays, 0);
            }
        }

        protected void WriteEnd(Stream stream, IpcOptions ipcOptions)
        {
            if (!ipcOptions.WriteLegacyIpcFormat)
            {
                SerDe.Write(stream, -1);
            }

            SerDe.Write(stream, 0);
        }
    }

    internal class ArrowOrDataFrameSqlCommandExecutor : ArrowBasedCommandExecutor
    {
        internal ArrowOrDataFrameSqlCommandExecutor(Version version)
        {
            _version = version;
        }

        protected internal override CommandExecutorStat ExecuteCore(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            bool useDataFrameCommandExecutor = false;
            bool useArrowSqlCommandExecutor = false;
            foreach (SqlCommand command in commands)
            {
                WorkerFunction workerFunc = command.WorkerFunction;
                if (workerFunc is DataFrameWorkerFunction dataFrameWorkedFunc)
                {
                    useDataFrameCommandExecutor = true;
                }
                else
                {
                    useArrowSqlCommandExecutor = true;
                }
            }
            if (useDataFrameCommandExecutor && useArrowSqlCommandExecutor)
            {
                // Mixed mode. Not supported
                throw new NotSupportedException("Combined Arrow and DataFrame style commands are not supported");
            }
            if (useDataFrameCommandExecutor)
            {
                return ExecuteDataFrameSqlCommand(inputStream, outputStream, commands);
            }
            return ExecuteArrowSqlCommand(inputStream, outputStream, commands);
        }

        private CommandExecutorStat ExecuteArrowSqlCommand(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            var stat = new CommandExecutorStat();
            ICommandRunner commandRunner = CreateCommandRunner(commands);

            SerDe.Write(outputStream, (int)SpecialLengths.START_ARROW_STREAM);

            IpcOptions ipcOptions = ArrowIpcOptions();
            ArrowStreamWriter writer = null;
            Schema resultSchema = null;
            foreach (ReadOnlyMemory<IArrowArray> input in GetArrowInputIterator(inputStream))
            {
                IArrowArray[] results = commandRunner.Run(input);

                // Assumes all columns have the same length, so uses 0th for num entries.
                int numEntries = results[0].Length;
                stat.NumEntriesProcessed += numEntries;

                if (writer == null)
                {
                    Debug.Assert(resultSchema == null);
                    resultSchema = BuildSchema(results);

                    writer =
                        new ArrowStreamWriter(outputStream, resultSchema, leaveOpen: true, ipcOptions);
                }

                var recordBatch = new RecordBatch(resultSchema, results, numEntries);

                writer.WriteRecordBatch(recordBatch);
            }

            WriteEnd(outputStream, ipcOptions);
            writer?.Dispose();

            return stat;
        }

        private CommandExecutorStat ExecuteDataFrameSqlCommand(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            var stat = new CommandExecutorStat();
            ICommandRunner commandRunner = CreateCommandRunner(commands);

            SerDe.Write(outputStream, (int)SpecialLengths.START_ARROW_STREAM);

            IpcOptions ipcOptions = ArrowIpcOptions();
            ArrowStreamWriter writer = null;
            foreach (RecordBatch input in GetInputIterator(inputStream))
            {
                FxDataFrame dataFrame = FxDataFrame.FromArrowRecordBatch(input);
                var inputColumns = new DataFrameColumn[input.ColumnCount];
                for (int i = 0; i < dataFrame.Columns.Count; ++i)
                {
                    inputColumns[i] = dataFrame.Columns[i];
                }

                DataFrameColumn[] results = commandRunner.Run(inputColumns);

                var resultDataFrame = new FxDataFrame(results);
                IEnumerable<RecordBatch> recordBatches = resultDataFrame.ToArrowRecordBatches();

                foreach (RecordBatch result in recordBatches)
                {
                    stat.NumEntriesProcessed += result.Length;

                    if (writer == null)
                    {
                        writer =
                            new ArrowStreamWriter(outputStream, result.Schema, leaveOpen: true, ipcOptions);
                    }

                    writer.WriteRecordBatch(result);
                }
            }

            WriteEnd(outputStream, ipcOptions);
            writer?.Dispose();

            return stat;
        }

        /// <summary>
        /// Create input iterator from the given input stream.
        /// </summary>
        /// <param name="inputStream">Stream to read from</param>
        /// <returns></returns>
        private IEnumerable<ReadOnlyMemory<IArrowArray>> GetArrowInputIterator(Stream inputStream)
        {
            IArrowArray[] arrays = null;
            int columnCount = 0;
            try
            {
                using var reader = new ArrowStreamReader(inputStream, leaveOpen: true);
                RecordBatch batch;
                while ((batch = reader.ReadNextRecordBatch()) != null)
                {
                    columnCount = batch.ColumnCount;
                    if (arrays == null)
                    {
                        // Note that every batch in a stream has the same schema.
                        arrays = ArrayPool<IArrowArray>.Shared.Rent(columnCount);
                    }

                    for (int i = 0; i < columnCount; ++i)
                    {
                        arrays[i] = batch.Column(i);
                    }

                    yield return new ReadOnlyMemory<IArrowArray>(arrays, 0, columnCount);
                }

                if (arrays == null)
                {
                    // When no input batches were received, return empty IArrowArrays
                    // in order to create and write back the result schema.
                    columnCount = reader.Schema.FieldsList.Count;
                    arrays = ArrayPool<IArrowArray>.Shared.Rent(columnCount);

                    for (int i = 0; i < columnCount; ++i)
                    {
                        arrays[i] = null;
                    }
                    yield return new ReadOnlyMemory<IArrowArray>(arrays, 0, columnCount);
                }
            }
            finally
            {
                if (arrays != null)
                {
                    arrays.AsSpan(0, columnCount).Clear();
                    ArrayPool<IArrowArray>.Shared.Return(arrays);
                }
            }
        }

        private static Schema BuildSchema(IArrowArray[] resultColumns)
        {
            var schemaBuilder = new Schema.Builder();
            if (resultColumns.Length == 1)
            {
                schemaBuilder = schemaBuilder
                    .Field(f => f.Name("Result")
                    .DataType(resultColumns[0].Data.DataType)
                    .Nullable(false));
            }
            else
            {
                for (int i = 0; i < resultColumns.Length; ++i)
                {
                    schemaBuilder = schemaBuilder
                        .Field(f => f.Name("Result" + i)
                        .DataType(resultColumns[i].Data.DataType)
                        .Nullable(false));
                }
            }
            return schemaBuilder.Build();
        }

        /// <summary>
        /// Creates an ICommandRunner instance based on the given commands.
        /// </summary>
        /// <param name="commands">Commands used for creating a command runner</param>
        /// <returns>An ICommandRunner instance</returns>
        private static ICommandRunner CreateCommandRunner(SqlCommand[] commands)
        {
            return (commands.Length == 1) ?
                (ICommandRunner)new SingleCommandRunner(commands[0]) :
                new MultiCommandRunner(commands);
        }

        /// <summary>
        /// Interface for running commands.
        /// On the Spark side, the following is expected for the Pickling to work:
        /// If there is a single command (one UDF), the computed value is returned
        /// as an object (one element). If there are multiple commands (multiple UDF scenario),
        /// the computed value should be an array (not IEnumerable) where each element
        /// in the array corresponds to the value returned by a command.
        /// Refer to EvaluatePython.scala for StructType case.
        /// </summary>
        private interface ICommandRunner
        {
            /// <summary>
            /// Runs commands based on the given split id and input.
            /// </summary>
            /// <param name="input">Input data for the commands to run</param>
            /// <returns>Value returned by running the commands</returns>
            IArrowArray[] Run(ReadOnlyMemory<IArrowArray> input);

            /// <summary>
            /// Runs commands based on the given split id and input.
            /// </summary>
            /// <param name="input">Input data for the commands to run</param>
            /// <returns>Value returned by running the commands</returns>
            DataFrameColumn[] Run(ReadOnlyMemory<DataFrameColumn> input);
        }

        /// <summary>
        /// SingleCommandRunner handles running a single command.
        /// </summary>
        private sealed class SingleCommandRunner : ICommandRunner
        {
            /// <summary>
            /// A command to run.
            /// </summary>
            private readonly SqlCommand _command;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="command">A command to run</param>
            internal SingleCommandRunner(SqlCommand command)
            {
                _command = command;
            }

            /// <summary>
            /// Runs a single command.
            /// </summary>
            /// <param name="input">Input data for the command to run</param>
            /// <returns>Value returned by running the command</returns>
            public IArrowArray[] Run(ReadOnlyMemory<IArrowArray> input)
            {
                return new[] { ((ArrowWorkerFunction)_command.WorkerFunction).Func(
                    input,
                    _command.ArgOffsets) };
            }

            /// <summary>
            /// Runs a single command.
            /// </summary>
            /// <param name="input">Input data for the command to run</param>
            /// <returns>Value returned by running the command</returns>
            public DataFrameColumn[] Run(ReadOnlyMemory<DataFrameColumn> input)
            {
                return new[] { ((DataFrameWorkerFunction)_command.WorkerFunction).Func(
                    input,
                    _command.ArgOffsets) };
            }
        }

        /// <summary>
        /// MultiCommandRunner handles running multiple commands.
        /// </summary>
        private sealed class MultiCommandRunner : ICommandRunner
        {
            /// <summary>
            /// Commands to run.
            /// </summary>
            private readonly SqlCommand[] _commands;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="commands">Multiple commands top run</param>
            internal MultiCommandRunner(SqlCommand[] commands)
            {
                _commands = commands;
            }

            /// <summary>
            /// Runs multiple commands.
            /// </summary>
            /// <param name="input">Input data for the commands to run</param>
            /// <returns>An array of values returned by running the commands</returns>
            public IArrowArray[] Run(ReadOnlyMemory<IArrowArray> input)
            {
                var resultColumns = new IArrowArray[_commands.Length];
                for (int i = 0; i < resultColumns.Length; ++i)
                {
                    SqlCommand command = _commands[i];
                    resultColumns[i] = ((ArrowWorkerFunction)command.WorkerFunction).Func(
                        input,
                        command.ArgOffsets);
                }
                return resultColumns;
            }

            /// <summary>
            /// Runs multiple commands.
            /// </summary>
            /// <param name="input">Input data for the commands to run</param>
            /// <returns>An array of values returned by running the commands</returns>
            public DataFrameColumn[] Run(ReadOnlyMemory<DataFrameColumn> input)
            {
                var resultColumns = new DataFrameColumn[_commands.Length];
                for (int i = 0; i < resultColumns.Length; ++i)
                {
                    SqlCommand command = _commands[i];
                    DataFrameColumn column = ((DataFrameWorkerFunction)command.WorkerFunction).Func(
                        input,
                        command.ArgOffsets);
                    column.SetName(column.Name + i);
                    resultColumns[i] = column;
                }
                return resultColumns;
            }
        }
    }

    internal class ArrowOrDataFrameGroupedMapCommandExecutor : ArrowOrDataFrameSqlCommandExecutor
    {
        internal ArrowOrDataFrameGroupedMapCommandExecutor(Version version)
            : base(version)
        {
        }

        protected internal override CommandExecutorStat ExecuteCore(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            Debug.Assert(commands.Length == 1,
                "Grouped Map UDFs do not support combining multiple UDFs.");

            bool useDataFrameGroupedMapCommandExecutor = false;
            bool useArrowGroupedMapCommandExecutor = false;
            foreach (SqlCommand command in commands)
            {
                if (command.WorkerFunction is DataFrameGroupedMapWorkerFunction groupedMapWorkerFunction)
                {
                    useDataFrameGroupedMapCommandExecutor = true;
                }
                else
                {
                    useArrowGroupedMapCommandExecutor = true;
                }
            }
            if (useDataFrameGroupedMapCommandExecutor && useArrowGroupedMapCommandExecutor)
            {
                // Mixed mode. Not supported
                throw new NotSupportedException("Combined Arrow and DataFrame style commands are not supported");
            }
            if (useDataFrameGroupedMapCommandExecutor)
            {
                return ExecuteDataFrameGroupedMapCommand(inputStream, outputStream, commands);
            }
            return ExecuteArrowGroupedMapCommand(inputStream, outputStream, commands);
        }

        private RecordBatch WrapColumnsInStructIfApplicable(RecordBatch batch)
        {
            if (_version >= new Version(Versions.V3_0_0))
            {
                var fields = new Field[batch.Schema.FieldsList.Count];
                for (int i = 0; i < batch.Schema.FieldsList.Count; ++i)
                {
                    fields[i] = batch.Schema.GetFieldByIndex(i);
                }

                var structType = new StructType(fields);
                var structArray = new StructArray(
                    structType,
                    batch.Length,
                    batch.Arrays.Cast<Apache.Arrow.Array>(),
                    ArrowBuffer.Empty);
                Schema schema = new Schema.Builder().Field(new Field("Struct", structType, false)).Build();
                return new RecordBatch(schema, new[] { structArray }, batch.Length);
            }

            return batch;
        }

        private CommandExecutorStat ExecuteArrowGroupedMapCommand(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            Debug.Assert(commands.Length == 1,
                "Grouped Map UDFs do not support combining multiple UDFs.");

            var stat = new CommandExecutorStat();
            var worker = (ArrowGroupedMapWorkerFunction)commands[0].WorkerFunction;

            SerDe.Write(outputStream, (int)SpecialLengths.START_ARROW_STREAM);

            IpcOptions ipcOptions = ArrowIpcOptions();
            ArrowStreamWriter writer = null;
            foreach (RecordBatch input in GetInputIterator(inputStream))
            {
                RecordBatch batch = worker.Func(input);

                RecordBatch final = WrapColumnsInStructIfApplicable(batch);
                int numEntries = final.Length;
                stat.NumEntriesProcessed += numEntries;

                if (writer == null)
                {
                    writer =
                        new ArrowStreamWriter(outputStream, final.Schema, leaveOpen: true, ipcOptions);
                }

                writer.WriteRecordBatch(final);
            }

            WriteEnd(outputStream, ipcOptions);
            writer?.Dispose();

            return stat;
        }

        private CommandExecutorStat ExecuteDataFrameGroupedMapCommand(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            Debug.Assert(commands.Length == 1,
                "Grouped Map UDFs do not support combining multiple UDFs.");

            var stat = new CommandExecutorStat();
            var worker = (DataFrameGroupedMapWorkerFunction)commands[0].WorkerFunction;

            SerDe.Write(outputStream, (int)SpecialLengths.START_ARROW_STREAM);

            IpcOptions ipcOptions = ArrowIpcOptions();
            ArrowStreamWriter writer = null;
            foreach (RecordBatch input in GetInputIterator(inputStream))
            {
                FxDataFrame dataFrame = FxDataFrame.FromArrowRecordBatch(input);
                FxDataFrame resultDataFrame = worker.Func(dataFrame);

                IEnumerable<RecordBatch> recordBatches = resultDataFrame.ToArrowRecordBatches();

                foreach (RecordBatch batch in recordBatches)
                {
                    RecordBatch final = WrapColumnsInStructIfApplicable(batch);
                    stat.NumEntriesProcessed += final.Length;

                    if (writer == null)
                    {
                        writer =
                            new ArrowStreamWriter(outputStream, final.Schema, leaveOpen: true, ipcOptions);
                    }

                    writer.WriteRecordBatch(final);
                }
            }

            WriteEnd(outputStream, ipcOptions);
            writer?.Dispose();

            return stat;
        }
    }
}
