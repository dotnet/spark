// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Spark.Network;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class TaskRunnerTests
    {
        [Fact]
        public void TestTaskRunner()
        {
            using var serverListener = new DefaultSocketWrapper();
            serverListener.Listen();

            var port = (serverListener.LocalEndPoint as IPEndPoint).Port;
            var clientSocket = new DefaultSocketWrapper();
            clientSocket.Connect(IPAddress.Loopback, port, null);

            PayloadWriter payloadWriter = new PayloadWriterFactory().Create();
            var taskRunner = new TaskRunner(0, clientSocket, false, payloadWriter.Version);
            Task clientTask = Task.Run(() => taskRunner.Run());

            using (ISocketWrapper serverSocket = serverListener.Accept())
            {
                TestTaskRunnerReadWrite(serverSocket, payloadWriter);
            }

            Assert.True(clientTask.Wait(5000));
        }

        internal static void TestTaskRunnerReadWrite(
            ISocketWrapper serverSocket,
            PayloadWriter payloadWriter)
        {
            System.IO.Stream inputStream = serverSocket.InputStream;
            System.IO.Stream outputStream = serverSocket.OutputStream;

            payloadWriter.WriteTestData(outputStream);
            // Now process the bytes flowing in from the client.
            List<object[]> rowsReceived = PayloadReader.Read(inputStream);

            // Validate rows received.
            Assert.Equal(10, rowsReceived.Count);
            for (int i = 0; i < 10; ++i)
            {
                // Two UDFs registered, thus expecting two columns.
                // Refer to TestData.GetDefaultCommandPayload().
                object[] row = rowsReceived[i];
                Assert.Equal(2, rowsReceived[i].Length);
                Assert.Equal($"udf2 udf1 {i}", row[0]);
                Assert.Equal(i + i, row[1]);
            }
        }
    }
}
