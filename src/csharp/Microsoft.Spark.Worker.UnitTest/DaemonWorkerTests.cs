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
    [Collection("Spark Unit Tests")]
    public class DaemonWorkerTests
    {
        [Theory]
        [MemberData(nameof(TestData.versionTests), MemberType = typeof(TestData))]
        public void TestsDaemonWorkerTaskRunners(string version)
        {
            ISocketWrapper daemonSocket = SocketFactory.CreateSocket();
            
            int taskRunnerNumber = 3;
            var typedVersion = new Version(version);
            var daemonWorker = new DaemonWorker(typedVersion);
            
            Task.Run(() => daemonWorker.Run(daemonSocket));

            for (int i = 0; i < taskRunnerNumber; ++i)
            {
                CreateAndVerifyConnection(daemonSocket, typedVersion);
            }
            
            Assert.Equal(taskRunnerNumber, daemonWorker.CurrentNumTaskRunners);
        }

        private static void CreateAndVerifyConnection(ISocketWrapper daemonSocket, Version version)
        {
            var ipEndpoint = (IPEndPoint)daemonSocket.LocalEndPoint;
            int port = ipEndpoint.Port;
            ISocketWrapper clientSocket = SocketFactory.CreateSocket();
            clientSocket.Connect(ipEndpoint.Address, port);

            // Now process the bytes flowing in from the client.
            PayloadWriter payloadWriter = new PayloadWriterFactory().Create(version);
            payloadWriter.WriteTestData(clientSocket.OutputStream);
            List<object[]> rowsReceived = PayloadReader.Read(clientSocket.InputStream);

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
