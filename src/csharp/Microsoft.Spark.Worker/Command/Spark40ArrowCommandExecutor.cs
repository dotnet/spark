// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Services;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;

namespace Microsoft.Spark.Worker.Command
{
    /// <summary>
    /// Spark 4 Arrow execution, including grouped input projection and the boundary between
    /// Arrow IPC messages and Spark control frames. Legacy executors retain their old behavior.
    /// </summary>
    internal sealed class Spark40ArrowCommandExecutor : SqlCommandExecutor
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(Spark40ArrowCommandExecutor));

        private readonly bool _groupedMap;

        internal Spark40ArrowCommandExecutor(bool groupedMap)
        {
            _groupedMap = groupedMap;
        }

        protected internal override CommandExecutorStat ExecuteCore(
            Stream inputStream,
            Stream outputStream,
            SqlCommand[] commands)
        {
            ValidateCommands(commands);
            var stat = new CommandExecutorStat();
            var output = new ArrowOutput(outputStream);
            ArrowStreamReader reader = null;
            RecordBatch input = null;
            try
            {
                reader = new ArrowStreamReader(inputStream, leaveOpen: true);
                // ArrowStreamReader loads its schema lazily on the first read, including
                // a schema-only stream whose first result is null.
                input = reader.ReadNextRecordBatch();
                foreach (SqlCommand command in commands)
                {
                    ValidateOffsets(command.ArgOffsets, reader.Schema.FieldsList.Count);
                    if (_groupedMap)
                    {
                        ValidateOffsets(command.GroupingKeyOffsets, reader.Schema.FieldsList.Count);
                    }
                }

                while (input != null)
                {
                    try
                    {
                        if (_groupedMap)
                        {
                            ExecuteGroup(input, commands[0], output, stat);
                        }
                        else
                        {
                            ExecuteScalar(input, commands, output, stat);
                        }
                    }
                    finally
                    {
                        // Delegates may return arrays that alias input buffers. Release input
                        // only after every result for this batch has been synchronously written.
                        DisposeQuietly(input);
                        input = null;
                    }
                    input = reader.ReadNextRecordBatch();
                }

                // Schema-only input does not represent a batch or a group.
                output.End();
                return stat;
            }
            catch (Exception exception)
            {
                if (output.HasFailed)
                {
                    throw new ArrowStreamWriteException(exception);
                }

                try
                {
                    // A user/validation failure between batches must finish valid IPC before
                    // TaskRunner writes PYTHON_EXCEPTION_THROWN. Never repair a partial write.
                    output.End();
                }
                catch (Exception cleanupException)
                {
                    s_logger.LogWarn($"Ending Arrow output failed: {cleanupException.GetType().Name}.");
                    throw new ArrowStreamWriteException(exception);
                }

                throw;
            }
            finally
            {
                DisposeQuietly(input);
                DisposeQuietly(output);
                DisposeQuietly(reader);
            }
        }

        private void ValidateCommands(SqlCommand[] commands)
        {
            if (_groupedMap &&
                (commands.Length != 1 || commands[0].ReturnSchema is null))
            {
                throw new InvalidDataException("Spark 4 grouped-map requires one command and a return schema.");
            }

            Type firstType = commands[0].WorkerFunction?.GetType();
            foreach (SqlCommand command in commands)
            {
                WorkerFunction function = command.WorkerFunction;
                bool supported = _groupedMap ?
                    function is ArrowGroupedMapWorkerFunction ||
                        function is DataFrameGroupedMapWorkerFunction :
                    function is ArrowWorkerFunction || function is DataFrameWorkerFunction;
                if (!supported || function.GetType() != firstType)
                {
                    throw new NotSupportedException(
                        "Combined Arrow and DataFrame style commands or mismatched evaluation types are not supported.");
                }
            }
        }

        private static void ValidateOffsets(int[] offsets, int columnCount)
        {
            if (offsets is null || offsets.Any(offset => offset < 0 || offset >= columnCount))
            {
                throw new InvalidDataException("Spark 4 Arrow argument offset is outside the input schema.");
            }
        }

        private static void ExecuteScalar(
            RecordBatch input,
            SqlCommand[] commands,
            ArrowOutput output,
            CommandExecutorStat stat)
        {
            if (commands[0].WorkerFunction is ArrowWorkerFunction)
            {
                var results = new IArrowArray[commands.Length];
                try
                {
                    IArrowArray[] columns = input.Arrays.ToArray();
                    for (int i = 0; i < commands.Length; ++i)
                    {
                        results[i] = ((ArrowWorkerFunction)commands[i].WorkerFunction).Func(
                            columns, commands[i].ArgOffsets);
                        ValidateScalarLength(results[i]?.Length, input.Length);
                    }

                    var batch = new RecordBatch(ScalarSchema(results), results, input.Length);
                    output.Write(batch);
                    stat.NumEntriesProcessed += batch.Length;
                }
                finally
                {
                    DisposeResultArrays(results, input);
                }
            }
            else
            {
                FxDataFrame dataFrame = FxDataFrame.FromArrowRecordBatch(input);
                DataFrameColumn[] columns = dataFrame.Columns.ToArray();
                var results = new DataFrameColumn[commands.Length];
                var names = new HashSet<string>(StringComparer.Ordinal);
                for (int i = 0; i < commands.Length; ++i)
                {
                    DataFrameColumn result = ((DataFrameWorkerFunction)commands[i].WorkerFunction)
                        .Func(columns, commands[i].ArgOffsets);
                    ValidateScalarLength(result?.Length, input.Length);
                    if (!names.Add(result.Name))
                    {
                        // Do not rename a user's input/aliased result just to satisfy the
                        // DataFrame's unique-name restriction for parallel scalar UDFs.
                        result = result.Clone();
                        string name = "Result" + i;
                        while (!names.Add(name))
                        {
                            name += "_";
                        }
                        result.SetName(name);
                    }
                    results[i] = result;
                }

                foreach (RecordBatch batch in new FxDataFrame(results).ToArrowRecordBatches())
                {
                    try
                    {
                        IArrowArray[] arrays = batch.Arrays.ToArray();
                        if (arrays.Length != results.Length || arrays.Any(a => a.Length != batch.Length))
                        {
                            throw new InvalidDataException("Invalid Spark 4 DataFrame Arrow conversion.");
                        }
                        output.Write(new RecordBatch(ScalarSchema(arrays), arrays, batch.Length));
                        stat.NumEntriesProcessed += batch.Length;
                    }
                    finally
                    {
                        DisposeQuietly(batch);
                    }
                }
            }
        }

        private static void ValidateScalarLength(long? actual, int expected)
        {
            if (actual != expected)
            {
                throw new InvalidDataException(
                    $"Spark 4 scalar Arrow UDF returned {actual?.ToString() ?? "null"} rows; expected {expected}.");
            }
        }

        private static Schema ScalarSchema(IArrowArray[] arrays)
        {
            return new Schema(arrays.Select((array, i) =>
                new Field("Result" + i, array.Data.DataType, nullable: true)), null);
        }

        private static void ExecuteGroup(
            RecordBatch input,
            SqlCommand command,
            ArrowOutput output,
            CommandExecutorStat stat)
        {
            // This is a borrowed view. Input owns its buffers until all group results are sent.
            var projected = new RecordBatch(
                new Schema(command.ArgOffsets.Select(input.Schema.GetFieldByIndex), input.Schema.Metadata),
                command.ArgOffsets.Select(input.Column),
                input.Length);

            if (command.WorkerFunction is ArrowGroupedMapWorkerFunction arrow)
            {
                RecordBatch result = null;
                try
                {
                    result = arrow.Func(projected);
                    WriteGroupResult(result, command, output, stat);
                }
                finally
                {
                    if (result != null && !ReferenceEquals(result, input) &&
                        !ReferenceEquals(result, projected))
                    {
                        DisposeQuietly(result);
                    }
                }
            }
            else
            {
                FxDataFrame dataFrame = FxDataFrame.FromArrowRecordBatch(projected);
                if (projected.ColumnCount == 0)
                {
                    // FromArrowRecordBatch infers row count from columns. Preserve real
                    // rows in a zero-value-column group using the public row append API.
                    for (int i = 0; i < projected.Length; ++i)
                    {
                        dataFrame.Append(System.Array.Empty<object>(), inPlace: true);
                    }
                }
                FxDataFrame result = ((DataFrameGroupedMapWorkerFunction)command.WorkerFunction)
                    .Func(dataFrame);
                if (result is null)
                {
                    throw new InvalidDataException("Spark 4 grouped-map UDF returned null.");
                }
                foreach (RecordBatch batch in result.ToArrowRecordBatches())
                {
                    try
                    {
                        WriteGroupResult(batch, command, output, stat);
                    }
                    finally
                    {
                        DisposeQuietly(batch);
                    }
                }
            }
        }

        private static void WriteGroupResult(
            RecordBatch result,
            SqlCommand command,
            ArrowOutput output,
            CommandExecutorStat stat)
        {
            ArrowOutputValidator.Validate(result, command.ReturnSchema);
            IReadOnlyList<Field> fields = result.Schema.FieldsList;
            if (command.WorkerFunction is DataFrameGroupedMapWorkerFunction)
            {
                // DataFrame derives nullable metadata from each group's actual NullCount.
                // Emit the stable Apply declaration after validating those actual nulls;
                // user-provided Arrow schemas still undergo strict stability checks.
                fields = fields.Select((field, i) => new Field(
                    field.Name, field.DataType, command.ReturnSchema.Fields[i].IsNullable,
                    field.Metadata)).ToArray();
            }
            var structType = new StructType(fields);
            var array = new StructArray(
                structType,
                result.Length,
                result.Arrays.Cast<Apache.Arrow.Array>(),
                ArrowBuffer.Empty);
            var schema = new Schema(new[] { new Field("Struct", structType, false) }, null);
            output.Write(new RecordBatch(schema, new[] { array }, result.Length));
            stat.NumEntriesProcessed += result.Length;
        }

        private static void DisposeResultArrays(IArrowArray[] results, RecordBatch input)
        {
            var seen = new HashSet<IArrowArray>(input.Arrays);
            foreach (IArrowArray result in results)
            {
                if (result != null && seen.Add(result))
                {
                    DisposeQuietly(result);
                }
            }
        }

        private static void DisposeQuietly(IDisposable disposable)
        {
            try
            {
                disposable?.Dispose();
            }
            catch (Exception exception)
            {
                s_logger.LogWarn($"Arrow cleanup failed: {exception.GetType().Name}.");
            }
        }

        private sealed class ArrowOutput : IDisposable
        {
            private enum OutputState { NotStarted, Writing, Ended, WriteFailed }

            private readonly Stream _stream;
            private OutputState _state;
            private ArrowStreamWriter _writer;
            private Schema _schema;

            internal ArrowOutput(Stream stream)
            {
                _stream = stream;
            }

            internal bool HasFailed => _state == OutputState.WriteFailed;

            internal void Write(RecordBatch batch)
            {
                if (_schema != null)
                {
                    ArrowOutputValidator.ValidateSchema(_schema, batch.Schema);
                }
                else
                {
                    _schema = batch.Schema;
                    _writer = new ArrowStreamWriter(
                        _stream, _schema, leaveOpen: true,
                        new IpcOptions { WriteLegacyIpcFormat = false });
                }

                try
                {
                    if (_state == OutputState.NotStarted)
                    {
                        SerDe.Write(_stream, (int)SpecialLengths.START_ARROW_STREAM);
                        _state = OutputState.Writing;
                    }
                    _writer.WriteRecordBatch(batch);
                }
                catch
                {
                    _state = OutputState.WriteFailed;
                    throw;
                }
            }

            internal void End()
            {
                if (_state != OutputState.Writing)
                {
                    return;
                }
                try
                {
                    _writer.WriteEnd();
                    _stream.Flush();
                    _state = OutputState.Ended;
                }
                catch
                {
                    _state = OutputState.WriteFailed;
                    throw;
                }
            }

            public void Dispose() => _writer?.Dispose();
        }
    }

    /// <summary>
    /// The output may contain a partial Arrow message; only closing the connection is safe.
    /// </summary>
    internal sealed class ArrowStreamWriteException : IOException
    {
        internal ArrowStreamWriteException(Exception primaryException)
            : base("Spark 4 Arrow output is incomplete; the worker connection cannot be reused.", primaryException)
        {
        }
    }
}
