// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Command;
using Razorvine.Pickle;
using Xunit;
using static Microsoft.Spark.UnitTest.TestUtils.ArrowTestUtils;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class CommandExecutorTests
    {
        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]
        public void TestPicklingSqlCommandExecutorWithSingleCommand(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            _ = ipcOptions;
            var udfWrapper = new Sql.PicklingUdfWrapper<string, string>(
                (str) => "udf: " + ((str is null) ? "NULL" : str));
            var command = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.PicklingWorkerFunction(udfWrapper.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_BATCHED_UDF,
                Commands = new[] { command }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            int numRows = 10;

            // Write test data to the input stream.
            var pickler = new Pickler();
            for (int i = 0; i < numRows; ++i)
            {
                byte[] pickled = pickler.dumps(
                    new[] { new object[] { (i % 2 == 0) ? null : i.ToString() } });
                SerDe.Write(inputStream, pickled.Length);
                SerDe.Write(inputStream, pickled);
            }
            SerDe.Write(inputStream, (int)SpecialLengths.END_OF_DATA_SECTION);
            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate that all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(10, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            var unpickler = new Unpickler();

            // One row was written as a batch above, thus need to read 'numRows' batches.
            List<object> rows = new List<object>();
            for (int i = 0; i < numRows; ++i)
            {
                int length = SerDe.ReadInt32(outputStream);
                byte[] pickledBytes = SerDe.ReadBytes(outputStream, length);
                rows.Add((unpickler.loads(pickledBytes) as ArrayList)[0] as object);
            }

            Assert.Equal(numRows, rows.Count);

            // Validate the single command.
            for (int i = 0; i < numRows; ++i)
            {
                Assert.Equal(
                    "udf: " + ((i % 2 == 0) ? "NULL" : i.ToString()),
                    (string)rows[i]);
            }

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]
        public void TestPicklingSqlCommandExecutorWithMultiCommands(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            _ = ipcOptions;
            var udfWrapper1 = new Sql.PicklingUdfWrapper<string, string>((str) => $"udf: {str}");
            var udfWrapper2 = new Sql.PicklingUdfWrapper<int, int, int>(
                (arg1, arg2) => arg1 * arg2);

            var command1 = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.PicklingWorkerFunction(udfWrapper1.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var command2 = new SqlCommand()
            {
                ArgOffsets = new[] { 1, 2 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.PicklingWorkerFunction(udfWrapper2.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_BATCHED_UDF,
                Commands = new[] { command1, command2 }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            int numRows = 10;

            // Write test data to the input stream.
            var pickler = new Pickler();
            for (int i = 0; i < numRows; ++i)
            {
                byte[] pickled = pickler.dumps(
                    new[] { new object[] { i.ToString(), i, i } });
                SerDe.Write(inputStream, pickled.Length);
                SerDe.Write(inputStream, pickled);
            }
            SerDe.Write(inputStream, (int)SpecialLengths.END_OF_DATA_SECTION);
            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(10, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            var unpickler = new Unpickler();

            // One row was written as a batch above, thus need to read 'numRows' batches.
            List<object[]> rows = new List<object[]>();
            for (int i = 0; i < numRows; ++i)
            {
                int length = SerDe.ReadInt32(outputStream);
                byte[] pickledBytes = SerDe.ReadBytes(outputStream, length);
                rows.Add((unpickler.loads(pickledBytes) as ArrayList)[0] as object[]);
            }

            Assert.Equal(numRows, rows.Count);

            for (int i = 0; i < numRows; ++i)
            {
                // There were two UDFs each of which produces one column.
                object[] columns = rows[i];
                Assert.Equal($"udf: {i}", (string)columns[0]);
                Assert.Equal(i * i, (int)columns[1]);
            }

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]
        public void TestPicklingSqlCommandExecutorWithEmptyInput(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            _ = ipcOptions;
            var udfWrapper = new Sql.PicklingUdfWrapper<string, string>((str) => $"udf: {str}");
            var command = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.PicklingWorkerFunction(udfWrapper.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_BATCHED_UDF,
                Commands = new[] { command }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            // Write test data to the input stream. For the empty input scenario,
            // only send SpecialLengths.END_OF_DATA_SECTION.
            SerDe.Write(inputStream, (int)SpecialLengths.END_OF_DATA_SECTION);
            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate that all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(0, stat.NumEntriesProcessed);

            // Validate the output stream.
            Assert.Equal(0, outputStream.Length);
        }

        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]
        public async Task TestArrowSqlCommandExecutorWithSingleCommand(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            var udfWrapper = new Sql.ArrowUdfWrapper<StringArray, StringArray>(
                (strings) => (StringArray)ToArrowArray(
                    Enumerable.Range(0, strings.Length)
                        .Select(i => $"udf: {strings.GetString(i)}")
                        .ToArray()));

            var command = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.ArrowWorkerFunction(udfWrapper.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                Commands = new[] { command }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            int numRows = 10;

            // Write test data to the input stream.
            Schema schema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Build();
            var arrowWriter =
                new ArrowStreamWriter(inputStream, schema, leaveOpen: false, ipcOptions);
            await arrowWriter.WriteRecordBatchAsync(
                new RecordBatch(
                    schema,
                    new[]
                    {
                        ToArrowArray(
                            Enumerable.Range(0, numRows)
                                .Select(i => i.ToString())
                                .ToArray())
                    },
                    numRows));

            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate that all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(numRows, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            int arrowLength = SerDe.ReadInt32(outputStream);
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, arrowLength);
            var arrowReader = new ArrowStreamReader(outputStream);
            RecordBatch outputBatch = await arrowReader.ReadNextRecordBatchAsync();

            Assert.Equal(numRows, outputBatch.Length);
            Assert.Single(outputBatch.Arrays);
            var array = (StringArray)outputBatch.Arrays.ElementAt(0);
            // Validate the single command.
            for (int i = 0; i < numRows; ++i)
            {
                Assert.Equal($"udf: {i}", array.GetString(i));
            }

            CheckEOS(outputStream, ipcOptions);

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]

        public async Task TestDataFrameSqlCommandExecutorWithSingleCommand(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            var udfWrapper = new Sql.DataFrameUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                (strings) => strings.Apply(cur => $"udf: {cur}"));

            var command = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.DataFrameWorkerFunction(udfWrapper.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                Commands = new[] { command }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            int numRows = 10;

            // Write test data to the input stream.
            Schema schema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Build();
            var arrowWriter =
                new ArrowStreamWriter(inputStream, schema, leaveOpen: false, ipcOptions);
            await arrowWriter.WriteRecordBatchAsync(
                new RecordBatch(
                    schema,
                    new[]
                    {
                        ToArrowArray(
                            Enumerable.Range(0, numRows)
                                .Select(i => i.ToString())
                                .ToArray())
                    },
                    numRows));

            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate that all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(numRows, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            int arrowLength = SerDe.ReadInt32(outputStream);
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, arrowLength);
            var arrowReader = new ArrowStreamReader(outputStream);
            RecordBatch outputBatch = await arrowReader.ReadNextRecordBatchAsync();

            Assert.Equal(numRows, outputBatch.Length);
            Assert.Single(outputBatch.Arrays);
            var array = (StringArray)outputBatch.Arrays.ElementAt(0);
            // Validate the single command.
            for (int i = 0; i < numRows; ++i)
            {
                Assert.Equal($"udf: {i}", array.GetString(i));
            }

            CheckEOS(outputStream, ipcOptions);

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]

        public async Task TestArrowSqlCommandExecutorWithMultiCommands(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            var udfWrapper1 = new Sql.ArrowUdfWrapper<StringArray, StringArray>(
                (strings) => (StringArray)ToArrowArray(
                    Enumerable.Range(0, strings.Length)
                        .Select(i => $"udf: {strings.GetString(i)}")
                        .ToArray()));
            var udfWrapper2 = new Sql.ArrowUdfWrapper<Int32Array, Int32Array, Int32Array>(
                (arg1, arg2) => (Int32Array)ToArrowArray(
                    Enumerable.Range(0, arg1.Length)
                        .Select(i => arg1.Values[i] * arg2.Values[i])
                        .ToArray()));

            var command1 = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.ArrowWorkerFunction(udfWrapper1.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var command2 = new SqlCommand()
            {
                ArgOffsets = new[] { 1, 2 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.ArrowWorkerFunction(udfWrapper2.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                Commands = new[] { command1, command2 }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            int numRows = 10;

            // Write test data to the input stream.
            Schema schema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Field(b => b.Name("arg2").DataType(Int32Type.Default))
                .Field(b => b.Name("arg3").DataType(Int32Type.Default))
                .Build();
            var arrowWriter =
                new ArrowStreamWriter(inputStream, schema, leaveOpen: false, ipcOptions);
            await arrowWriter.WriteRecordBatchAsync(
                new RecordBatch(
                    schema,
                    new[]
                    {
                        ToArrowArray(
                            Enumerable.Range(0, numRows)
                                .Select(i => i.ToString())
                                .ToArray()),
                        ToArrowArray(Enumerable.Range(0, numRows).ToArray()),
                        ToArrowArray(Enumerable.Range(0, numRows).ToArray()),
                    },
                    numRows));

            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(numRows, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            int arrowLength = SerDe.ReadInt32(outputStream);
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, arrowLength);
            var arrowReader = new ArrowStreamReader(outputStream);
            RecordBatch outputBatch = await arrowReader.ReadNextRecordBatchAsync();

            Assert.Equal(numRows, outputBatch.Length);
            Assert.Equal(2, outputBatch.Arrays.Count());
            var array1 = (StringArray)outputBatch.Arrays.ElementAt(0);
            var array2 = (Int32Array)outputBatch.Arrays.ElementAt(1);
            for (int i = 0; i < numRows; ++i)
            {
                Assert.Equal($"udf: {i}", array1.GetString(i));
                Assert.Equal(i * i, array2.Values[i]);
            }

            CheckEOS(outputStream, ipcOptions);

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]

        public async Task TestDataFrameSqlCommandExecutorWithMultiCommands(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            var udfWrapper1 = new Sql.DataFrameUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                (strings) => strings.Apply(cur => $"udf: {cur}"));

            var udfWrapper2 = new Sql.DataFrameUdfWrapper<Int32DataFrameColumn, Int32DataFrameColumn, Int32DataFrameColumn>(
                (arg1, arg2) => arg1 * arg2);

            var command1 = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.DataFrameWorkerFunction(udfWrapper1.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var command2 = new SqlCommand()
            {
                ArgOffsets = new[] { 1, 2 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.DataFrameWorkerFunction(udfWrapper2.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                Commands = new[] { command1, command2 }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            int numRows = 10;

            // Write test data to the input stream.
            Schema schema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Field(b => b.Name("arg2").DataType(Int32Type.Default))
                .Field(b => b.Name("arg3").DataType(Int32Type.Default))
                .Build();
            var arrowWriter =
                new ArrowStreamWriter(inputStream, schema, leaveOpen: false, ipcOptions);
            await arrowWriter.WriteRecordBatchAsync(
                new RecordBatch(
                    schema,
                    new[]
                    {
                        ToArrowArray(
                            Enumerable.Range(0, numRows)
                                .Select(i => i.ToString())
                                .ToArray()),
                        ToArrowArray(Enumerable.Range(0, numRows).ToArray()),
                        ToArrowArray(Enumerable.Range(0, numRows).ToArray()),
                    },
                    numRows));

            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(numRows, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            var arrowLength = SerDe.ReadInt32(outputStream);
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, arrowLength);
            var arrowReader = new ArrowStreamReader(outputStream);
            RecordBatch outputBatch = await arrowReader.ReadNextRecordBatchAsync();

            Assert.Equal(numRows, outputBatch.Length);
            Assert.Equal(2, outputBatch.Arrays.Count());
            var array1 = (StringArray)outputBatch.Arrays.ElementAt(0);
            var array2 = (Int32Array)outputBatch.Arrays.ElementAt(1);
            for (int i = 0; i < numRows; ++i)
            {
                Assert.Equal($"udf: {i}", array1.GetString(i));
                Assert.Equal(i * i, array2.Values[i]);
            }

            CheckEOS(outputStream, ipcOptions);

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        /// <summary>
        /// Tests when Spark writes an input stream that only contains a
        /// Schema, and no record batches, that CommandExecutor writes the
        /// appropriate response back.
        /// </summary>
        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]

        public void TestArrowSqlCommandExecutorWithEmptyInput(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            var udfWrapper = new Sql.ArrowUdfWrapper<StringArray, StringArray>(
                (strings) => (StringArray)ToArrowArray(
                    Enumerable.Range(0, strings.Length)
                        .Select(i => $"udf: {strings.GetString(i)}")
                        .ToArray()));

            var command = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.ArrowWorkerFunction(udfWrapper.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                Commands = new[] { command }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            // Write test data to the input stream.
            Schema schema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Build();
            var arrowWriter =
                new ArrowStreamWriter(inputStream, schema, leaveOpen: false, ipcOptions);

            // The .NET ArrowStreamWriter doesn't currently support writing just a 
            // schema with no batches - but Java does. We use Reflection to simulate
            // the request Spark sends.
            MethodInfo writeSchemaMethod = arrowWriter.GetType().GetMethod(
                "WriteSchemaAsync",
                BindingFlags.NonPublic | BindingFlags.Instance);

            writeSchemaMethod.Invoke(
                arrowWriter,
                new object[] { schema, CancellationToken.None });

            SerDe.Write(inputStream, 0);

            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate that all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(0, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            int arrowLength = SerDe.ReadInt32(outputStream);
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, arrowLength);
            var arrowReader = new ArrowStreamReader(outputStream);
            RecordBatch outputBatch = arrowReader.ReadNextRecordBatch();

            Assert.Equal(1, outputBatch.Schema.FieldsList.Count);
            Assert.IsType<StringType>(outputBatch.Schema.GetFieldByIndex(0).DataType);

            Assert.Equal(0, outputBatch.Length);
            Assert.Single(outputBatch.Arrays);

            var array = (StringArray)outputBatch.Arrays.ElementAt(0);
            Assert.Equal(0, array.Length);

            CheckEOS(outputStream, ipcOptions);

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        /// <summary>
        /// Tests when Spark writes an input stream that only contains a
        /// Schema, and no record batches, that CommandExecutor writes the
        /// appropriate response back.
        /// </summary>
        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]

        public void TestDataFrameSqlCommandExecutorWithEmptyInput(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            var udfWrapper = new Sql.DataFrameUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                (strings) => strings.Apply(cur => $"udf: {cur}"));

            var command = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.DataFrameWorkerFunction(udfWrapper.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                Commands = new[] { command }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            // Write test data to the input stream.
            Schema schema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Build();
            var arrowWriter = new ArrowStreamWriter(inputStream, schema, false, ipcOptions);

            // The .NET ArrowStreamWriter doesn't currently support writing just a 
            // schema with no batches - but Java does. We use Reflection to simulate
            // the request Spark sends.
            MethodInfo writeSchemaMethod = arrowWriter.GetType().GetMethod(
                "WriteSchemaAsync",
                BindingFlags.NonPublic | BindingFlags.Instance);

            writeSchemaMethod.Invoke(
                arrowWriter,
                new object[] { schema, CancellationToken.None });

            SerDe.Write(inputStream, 0);

            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate that all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(0, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            int arrowLength = SerDe.ReadInt32(outputStream);
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, arrowLength);
            var arrowReader = new ArrowStreamReader(outputStream);
            RecordBatch outputBatch = arrowReader.ReadNextRecordBatch();

            Assert.Equal(1, outputBatch.Schema.FieldsList.Count);
            Assert.IsType<StringType>(outputBatch.Schema.GetFieldByIndex(0).DataType);

            Assert.Equal(0, outputBatch.Length);
            Assert.Single(outputBatch.Arrays);

            var array = (StringArray)outputBatch.Arrays.ElementAt(0);
            Assert.Equal(0, array.Length);

            CheckEOS(outputStream, ipcOptions);

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]

        public async Task TestArrowGroupedMapCommandExecutor(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            StringArray ConvertStrings(StringArray strings)
            {
                return (StringArray)ToArrowArray(
                    Enumerable.Range(0, strings.Length)
                        .Select(i => $"udf: {strings.GetString(i)}")
                        .ToArray());
            }

            Int64Array ConvertInt64s(Int64Array int64s)
            {
                return (Int64Array)ToArrowArray(
                    Enumerable.Range(0, int64s.Length)
                        .Select(i => int64s.Values[i] + 100)
                        .ToArray());
            }

            Schema resultSchema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Field(b => b.Name("arg2").DataType(Int64Type.Default))
                .Build();

            var udfWrapper = new Sql.ArrowGroupedMapUdfWrapper(
                (batch) => new RecordBatch(
                    resultSchema,
                    new IArrowArray[]
                    {
                        ConvertStrings((StringArray)batch.Column(0)),
                        ConvertInt64s((Int64Array)batch.Column(1)),
                    },
                    batch.Length));

            var command = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.ArrowGroupedMapWorkerFunction(udfWrapper.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                Commands = new[] { command }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            int numRows = 10;

            // Write test data to the input stream.
            Schema schema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Field(b => b.Name("arg2").DataType(Int64Type.Default))
                .Build();
            var arrowWriter =
                new ArrowStreamWriter(inputStream, schema, leaveOpen: false, ipcOptions);
            await arrowWriter.WriteRecordBatchAsync(
                new RecordBatch(
                    schema,
                    new[]
                    {
                        ToArrowArray(
                            Enumerable.Range(0, numRows)
                                .Select(i => i.ToString())
                                .ToArray()),
                        ToArrowArray(
                            Enumerable.Range(0, numRows)
                                .Select(i => (long)i)
                                .ToArray())
                    },
                    numRows));

            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate that all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(numRows, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            int arrowLength = SerDe.ReadInt32(outputStream);
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, arrowLength);
            var arrowReader = new ArrowStreamReader(outputStream);
            RecordBatch outputBatch = await arrowReader.ReadNextRecordBatchAsync();

            Assert.Equal(numRows, outputBatch.Length);
            StringArray stringArray;
            Int64Array longArray;
            if (sparkVersion < new Version(Versions.V3_0_0))
            {
                Assert.Equal(2, outputBatch.ColumnCount);
                stringArray = (StringArray)outputBatch.Column(0);
                longArray = (Int64Array)outputBatch.Column(1);
            }
            else
            {
                Assert.Equal(1, outputBatch.ColumnCount);
                var structArray = (StructArray)outputBatch.Column(0);
                Assert.Equal(2, structArray.Fields.Count);
                stringArray = (StringArray)structArray.Fields[0];
                longArray = (Int64Array)structArray.Fields[1];
            }

            for (int i = 0; i < numRows; ++i)
            {
                Assert.Equal($"udf: {i}", stringArray.GetString(i));
            }

            for (int i = 0; i < numRows; ++i)
            {
                Assert.Equal(100 + i, longArray.Values[i]);
            }

            CheckEOS(outputStream, ipcOptions);

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]

        public async Task TestDataFrameGroupedMapCommandExecutor(
            Version sparkVersion,
            IpcOptions ipcOptions)
        {
            static ArrowStringDataFrameColumn ConvertStrings(ArrowStringDataFrameColumn strings)
            {
                return strings.Apply(cur => $"udf: {cur}");
            }

            Schema resultSchema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Field(b => b.Name("arg2").DataType(Int64Type.Default))
                .Build();

            var udfWrapper = new Sql.DataFrameGroupedMapUdfWrapper(
                (dataFrame) =>
                {
                    ArrowStringDataFrameColumn stringColumn = ConvertStrings(dataFrame.Columns.GetArrowStringColumn("arg1"));
                    DataFrameColumn doubles = dataFrame.Columns[1] + 100;
                    return new DataFrame(new List<DataFrameColumn>() { stringColumn, doubles });
                });

            var command = new SqlCommand()
            {
                ArgOffsets = new[] { 0 },
                NumChainedFunctions = 1,
                WorkerFunction = new Sql.DataFrameGroupedMapWorkerFunction(udfWrapper.Execute),
                SerializerMode = CommandSerDe.SerializedMode.Row,
                DeserializerMode = CommandSerDe.SerializedMode.Row
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                Commands = new[] { command }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
            int numRows = 10;

            // Write test data to the input stream.
            Schema schema = new Schema.Builder()
                .Field(b => b.Name("arg1").DataType(StringType.Default))
                .Field(b => b.Name("arg2").DataType(Int64Type.Default))
                .Build();
            var arrowWriter =
                new ArrowStreamWriter(inputStream, schema, leaveOpen: false, ipcOptions);
            await arrowWriter.WriteRecordBatchAsync(
                new RecordBatch(
                    schema,
                    new[]
                    {
                        ToArrowArray(
                            Enumerable.Range(0, numRows)
                                .Select(i => i.ToString())
                                .ToArray()),
                        ToArrowArray(
                            Enumerable.Range(0, numRows)
                                .Select(i => (long)i)
                                .ToArray())
                    },
                    numRows));

            inputStream.Seek(0, SeekOrigin.Begin);

            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate that all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(numRows, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);
            int arrowLength = SerDe.ReadInt32(outputStream);
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, arrowLength);
            var arrowReader = new ArrowStreamReader(outputStream);
            RecordBatch outputBatch = await arrowReader.ReadNextRecordBatchAsync();

            Assert.Equal(numRows, outputBatch.Length);
            StringArray stringArray;
            DoubleArray doubleArray;
            if (sparkVersion < new Version(Versions.V3_0_0))
            {
                Assert.Equal(2, outputBatch.ColumnCount);
                stringArray = (StringArray)outputBatch.Column(0);
                doubleArray = (DoubleArray)outputBatch.Column(1);
            }
            else
            {
                Assert.Equal(1, outputBatch.ColumnCount);
                var structArray = (StructArray)outputBatch.Column(0);
                Assert.Equal(2, structArray.Fields.Count);
                stringArray = (StringArray)structArray.Fields[0];
                doubleArray = (DoubleArray)structArray.Fields[1];
            }

            for (int i = 0; i < numRows; ++i)
            {
                Assert.Equal($"udf: {i}", stringArray.GetString(i));
            }

            for (int i = 0; i < numRows; ++i)
            {
                Assert.Equal(100 + i, doubleArray.Values[i]);
            }

            CheckEOS(outputStream, ipcOptions);

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        [Theory]
        [MemberData(nameof(CommandExecutorData.Data), MemberType = typeof(CommandExecutorData))]
        public void TestRDDCommandExecutor(Version sparkVersion, IpcOptions ipcOptions)
        {
            _ = ipcOptions;
            static int mapUdf(int a) => a + 3;
            var command = new RDDCommand()
            {
                WorkerFunction = new RDD.WorkerFunction(
                    new RDD<int>.MapUdfWrapper<int, int>(mapUdf).Execute),
                SerializerMode = CommandSerDe.SerializedMode.Byte,
                DeserializerMode = CommandSerDe.SerializedMode.Byte
            };

            var commandPayload = new Worker.CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.NON_UDF,
                Commands = new[] { command }
            };

            using var inputStream = new MemoryStream();
            using var outputStream = new MemoryStream();
#pragma warning disable SYSLIB0011 // Type or member is obsolete
            // Write test data to the input stream.
            var formatter = new BinaryFormatter();
#pragma warning restore SYSLIB0011 // Type or member is obsolete
            var memoryStream = new MemoryStream();

            var inputs = new int[] { 0, 1, 2, 3, 4 };

            var values = new List<byte[]>();
            foreach (int input in inputs)
            {
                memoryStream.Position = 0;
#pragma warning disable SYSLIB0011 // Type or member is obsolete
                // TODO: Replace BinaryFormatter with a new, secure serializer.
                formatter.Serialize(memoryStream, input);
#pragma warning restore SYSLIB0011 // Type or member is obsolete
                values.Add(memoryStream.ToArray());
            }

            foreach (byte[] value in values)
            {
                SerDe.Write(inputStream, value.Length);
                SerDe.Write(inputStream, value);
            }

            SerDe.Write(inputStream, (int)SpecialLengths.END_OF_DATA_SECTION);
            inputStream.Seek(0, SeekOrigin.Begin);

            // Execute the command.
            CommandExecutorStat stat = new CommandExecutor(sparkVersion).Execute(
                inputStream,
                outputStream,
                0,
                commandPayload);

            // Validate all the data on the stream is read.
            Assert.Equal(inputStream.Length, inputStream.Position);
            Assert.Equal(5, stat.NumEntriesProcessed);

            // Validate the output stream.
            outputStream.Seek(0, SeekOrigin.Begin);

            for (int i = 0; i < inputs.Length; ++i)
            {
                Assert.True(SerDe.ReadInt32(outputStream) > 0);
#pragma warning disable SYSLIB0011 // Type or member is obsolete
                // TODO: Replace BinaryFormatter with a new, secure serializer.
                Assert.Equal(
                    mapUdf(i),
                    formatter.Deserialize(outputStream));
#pragma warning restore SYSLIB0011 // Type or member is obsolete
            }

            // Validate all the data on the stream is read.
            Assert.Equal(outputStream.Length, outputStream.Position);
        }

        private void CheckEOS(Stream stream, IpcOptions ipcOptions)
        {
            if (!ipcOptions.WriteLegacyIpcFormat)
            {
                int continuationToken = SerDe.ReadInt32(stream);
                Assert.Equal(-1, continuationToken);
            }

            int end = SerDe.ReadInt32(stream);
            Assert.Equal(0, end);
        }
    }

    public class CommandExecutorData
    {
        // CommandExecutor only changes its behavior between major versions.
        public static IEnumerable<object[]> Data =>
            new List<object[]>
            {
                new object[]
                {
                    new Version(Versions.V2_4_2),
                    new IpcOptions
                    {
                        WriteLegacyIpcFormat = true
                    }
                },
                new object[]
                {
                    new Version(Versions.V3_0_0),
                    new IpcOptions
                    {
                        WriteLegacyIpcFormat = false
                    }
                }
            };
    }
}
