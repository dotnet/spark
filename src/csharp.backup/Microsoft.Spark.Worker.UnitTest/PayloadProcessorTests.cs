// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using System.Net;
using Microsoft.Spark.Network;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Processor;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class PayloadProcessorTests
    {
        [Theory]
        [MemberData(nameof(TestData.VersionData), MemberType = typeof(TestData))]
        public void TestPayloadProcessor(string version)
        {
            CommandPayload commandPayload = TestData.GetDefaultCommandPayload();
            PayloadWriter payloadWriter = new PayloadWriterFactory().Create(new Version(version));
            Payload payload = TestData.GetDefaultPayload();

            Payload actualPayload = null;
            using (var outStream = new MemoryStream())
            {
                payloadWriter.Write(outStream, payload, commandPayload);

                using var inputStream = new MemoryStream(outStream.ToArray());
                actualPayload = new PayloadProcessor(payloadWriter.Version).Process(inputStream);
            }

            // Validate the read payload.
            Assert.Equal(payload.SplitIndex, actualPayload.SplitIndex);
            Assert.Equal(payload.Version, actualPayload.Version);
            Assert.Equal(payload.TaskContext, actualPayload.TaskContext);
            Assert.Equal(payload.SparkFilesDir, actualPayload.SparkFilesDir);
            Assert.Equal(payload.IncludeItems, actualPayload.IncludeItems);
            Assert.Equal(payload.BroadcastVariables.Count, actualPayload.BroadcastVariables.Count);
            ValidateCommandPayload(commandPayload, actualPayload.Command);

            // Validate the UDFs.
            var actualCommand1 = (SqlCommand)actualPayload.Command.Commands[0];
            var result1 = ((PicklingWorkerFunction)actualCommand1.WorkerFunction).Func(
                0,
                new object[] { "hello", 10, 20 },
                actualCommand1.ArgOffsets);
            Assert.Equal("udf2 udf1 hello", result1);

            var actualCommand2 = (SqlCommand)actualPayload.Command.Commands[1];
            var result2 = ((PicklingWorkerFunction)actualCommand2.WorkerFunction).Func(
                0,
                new object[] { "hello", 10, 20 },
                actualCommand2.ArgOffsets);
            Assert.Equal(30, result2);
        }

        [Fact]
        public void TestClosedStreamWithSocket()
        {
            var commandPayload = new CommandPayload()
            {
                EvalType = UdfUtils.PythonEvalType.SQL_BATCHED_UDF,
                Commands = new Command[] { }
            };

            PayloadWriter payloadWriter = new PayloadWriterFactory().Create();
            Payload payload = TestData.GetDefaultPayload();

            using var serverListener = new DefaultSocketWrapper();
            serverListener.Listen();

            var port = (serverListener.LocalEndPoint as IPEndPoint).Port;
            using var clientSocket = new DefaultSocketWrapper();
            clientSocket.Connect(IPAddress.Loopback, port, null);

            using (ISocketWrapper serverSocket = serverListener.Accept())
            {
                Stream outStream = serverSocket.OutputStream;
                payloadWriter.Write(outStream, payload, commandPayload);
                outStream.Flush();
            }

            // At this point server socket is closed.
            Stream inStream = clientSocket.InputStream;

            // Consume bytes already written to the socket.
            var payloadProcessor = new PayloadProcessor(payloadWriter.Version);
            Payload actualPayload = payloadProcessor.Process(inStream);

            Assert.Equal(payload.SplitIndex, actualPayload.SplitIndex);
            Assert.Equal(payload.Version, actualPayload.Version);
            Assert.Equal(payload.TaskContext, actualPayload.TaskContext);
            Assert.Equal(payload.SparkFilesDir, actualPayload.SparkFilesDir);
            Assert.Equal(payload.IncludeItems, actualPayload.IncludeItems);
            Assert.Equal(payload.BroadcastVariables.Count, actualPayload.BroadcastVariables.Count);
            ValidateCommandPayload(commandPayload, actualPayload.Command);

            // Another read will detect that the socket is closed.
            Assert.Null(payloadProcessor.Process(inStream));
        }

        [Fact]
        public void TestClosedStreamWithMemoryStream()
        {
            var inputStream = new MemoryStream();

            // Version is not used in this scenario.
            var processor = new PayloadProcessor(null);

            // Nothing is written to the stream.
            Assert.Null(processor.Process(inputStream));

            inputStream.Dispose();

            // The stream is closed. Payload with null is expected.
            Assert.Null(processor.Process(inputStream));
        }

        private void ValidateCommandPayload(
            CommandPayload expected,
            Worker.CommandPayload actual)
        {
            Assert.Equal(expected.EvalType, actual.EvalType);
            Assert.Equal(expected.Commands.Length, actual.Commands.Count());

            for (int i = 0; i < expected.Commands.Length; ++i)
            {
                Command expectedCommand = expected.Commands[i];
                var actualCommand = (SqlCommand)actual.Commands[i];
                Assert.Equal(expectedCommand.ArgOffsets, actualCommand.ArgOffsets);
                Assert.Equal(
                    expectedCommand.ChainedUdfs.Length,
                    actualCommand.NumChainedFunctions);
                Assert.Equal(
                    expectedCommand.SerializerMode,
                    actualCommand.SerializerMode);
                Assert.Equal(
                    expectedCommand.DeserializerMode,
                    actualCommand.DeserializerMode);
            }
        }
    }
}
