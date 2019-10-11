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
using Microsoft.Data;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;
using Razorvine.Pickle;
using FxDataFrame = Microsoft.Data.DataFrame;

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
        /// <param name="inputStream">Input stream to read data from</param>
        /// <param name="outputStream">Output stream to write results to</param>
        /// <param name="evalType">Evaluation type for the current commands</param>
        /// <param name="commands">Contains the commands to execute</param>
        /// <returns>Statistics captured during the Execute() run</returns>
        internal static CommandExecutorStat Execute(
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
                executor = new ArrowSqlCommandExecutor();
            }
            else if (evalType == UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)
            {
                executor = new ArrowGroupedMapCommandExecutor();
            }
            else
            {
                throw new NotSupportedException($"{evalType} is not supported.");
            }

            return executor.ExecuteCore(inputStream, outputStream, commands);
        }

        protected IEnumerable<RecordBatch> GetInputIterator(Stream inputStream)
        {
            using (var reader = new ArrowStreamReader(inputStream, leaveOpen: true))
            {
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

                    int columnCount = reader.Schema.Fields.Count;
                    var arrays = new IArrowArray[columnCount];
                    for (int i = 0; i < columnCount; ++i)
                    {
                        IArrowType type = reader.Schema.GetFieldByIndex(i).DataType;
                        arrays[i] = ArrowArrayHelpers.CreateEmptyArray(type);
                    }
                    yield return new RecordBatch(reader.Schema, arrays, 0);
                }
            }
        }

        protected abstract CommandExecutorStat ExecuteCore(
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

        protected override CommandExecutorStat ExecuteCore(
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
            int messageLength = 0;
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
                        // Split id is not used for SQL UDFs, so 0 is passed.
                        outputRows.Add(commandRunner.Run(0, inputRows[i]));
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

            Pickler pickler = s_pickler ?? (s_pickler = new Pickler(false));
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

    /// <summary>
    /// A SqlCommandExecutor that reads and writes using the
    /// Apache Arrow format.
    /// </summary>
    internal class ArrowSqlCommandExecutor : SqlCommandExecutor
    {
        protected override CommandExecutorStat ExecuteCore(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            var stat = new CommandExecutorStat();
            ICommandRunner commandRunner = CreateCommandRunner(commands);

            SerDe.Write(outputStream, (int)SpecialLengths.START_ARROW_STREAM);

            ArrowStreamWriter writer = null;
            foreach (RecordBatch input in GetInputIterator(inputStream))
            {
                FxDataFrame dataFrame = new FxDataFrame(input);
                BaseColumn[] inputColumns = new BaseColumn[input.ColumnCount];
                for (int i = 0; i < dataFrame.ColumnCount; i++)
                {
                    inputColumns[i] = dataFrame.Column(i);
                }

                BaseColumn[] results = commandRunner.Run(inputColumns);

                FxDataFrame resultDataFrame = new FxDataFrame(results);
                IEnumerable<RecordBatch> recordBatches = resultDataFrame.AsArrowRecordBatches();

                foreach (RecordBatch result in recordBatches)
                {
                    int numEntries = result.Length;
                    stat.NumEntriesProcessed += numEntries;

                    if (writer == null)
                    {
                        writer = new ArrowStreamWriter(outputStream, result.Schema, leaveOpen: true);
                    }

                    // TODO: Remove sync-over-async once WriteRecordBatch exists.
                    writer.WriteRecordBatchAsync(result).GetAwaiter().GetResult();
                }
            }

            SerDe.Write(outputStream, 0);

            if (writer != null)
            {
                writer.Dispose();
            }

            return stat;
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
            BaseColumn[] Run(ReadOnlyMemory<BaseColumn> input);
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
            public BaseColumn[] Run(ReadOnlyMemory<BaseColumn> input)
            {
                return new[] { ((ArrowWorkerFunction)_command.WorkerFunction).Func(
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
            public BaseColumn[] Run(ReadOnlyMemory<BaseColumn> input)
            {
                var resultColumns = new BaseColumn[_commands.Length];
                for (int i = 0; i < resultColumns.Length; ++i)
                {
                    SqlCommand command = _commands[i];
                    BaseColumn column = ((ArrowWorkerFunction)command.WorkerFunction).Func(
                        input,
                        command.ArgOffsets);
                    column.SetName(column.Name + i);
                    resultColumns[i] = column;
                }
                return resultColumns;
            }
        }
    }

    internal class ArrowGroupedMapCommandExecutor : SqlCommandExecutor
    {
        protected override CommandExecutorStat ExecuteCore(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            Debug.Assert(commands.Length == 1,
                "Grouped Map UDFs do not support combining multiple UDFs.");

            var stat = new CommandExecutorStat();
            var worker = (ArrowGroupedMapWorkerFunction)commands[0].WorkerFunction;

            SerDe.Write(outputStream, (int)SpecialLengths.START_ARROW_STREAM);

            ArrowStreamWriter writer = null;
            foreach (RecordBatch input in GetInputIterator(inputStream))
            {
                FxDataFrame dataFrame = new FxDataFrame(input);
                FxDataFrame resultDataFrame = worker.Func(dataFrame);
                IEnumerable<RecordBatch> recordBatches = resultDataFrame.AsArrowRecordBatches();

                foreach (RecordBatch result in recordBatches)
                {
                    int numEntries = result.Length;
                    stat.NumEntriesProcessed += numEntries;

                    if (writer == null)
                    {
                        writer = new ArrowStreamWriter(outputStream, result.Schema, leaveOpen: true);
                    }

                    // TODO: Remove sync-over-async once WriteRecordBatch exists.
                    writer.WriteRecordBatchAsync(result).GetAwaiter().GetResult();
                }
            }

            SerDe.Write(outputStream, 0);

            if (writer != null)
            {
                writer.Dispose();
            }

            return stat;
        }
    }
}
