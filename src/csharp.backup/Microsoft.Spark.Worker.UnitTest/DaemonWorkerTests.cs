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
    public class DaemonWorkerTests : IDisposable
    {
        private const string ReuseWorkerEnvVariable = "SPARK_REUSE_WORKER";
        private readonly string _reuseWorker;

        public DaemonWorkerTests()
        {
            _reuseWorker = Environment.GetEnvironmentVariable(ReuseWorkerEnvVariable);
            Environment.SetEnvironmentVariable(ReuseWorkerEnvVariable, "1");
        }

        [Theory]
        [MemberData(nameof(TestData.VersionData), MemberType = typeof(TestData))]
        public void TestsDaemonWorkerTaskRunners(string version)
        {
            ISocketWrapper daemonSocket = SocketFactory.CreateSocket();
            
            int taskRunnerNumber = 2;
            var typedVersion = new Version(version);
            var daemonWorker = new DaemonWorker(typedVersion);
            
            Task.Run(() => daemonWorker.Run(daemonSocket));

            var clientSockets = new List<ISocketWrapper>();
            for (int i = 0; i < taskRunnerNumber; ++i)
            {
                CreateAndVerifyConnection(daemonSocket, clientSockets, typedVersion);
            }
            
            Assert.Equal(taskRunnerNumber, daemonWorker.CurrentNumTaskRunners);
        }

        private static void CreateAndVerifyConnection(
            ISocketWrapper daemonSocket,
            List<ISocketWrapper> clientSockets,
            Version version)
        {
            var ipEndpoint = (IPEndPoint)daemonSocket.LocalEndPoint;
            int port = ipEndpoint.Port;
            ISocketWrapper clientSocket = SocketFactory.CreateSocket();
            clientSockets.Add(clientSocket);
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

        public void Dispose()
        {
            Environment.SetEnvironmentVariable(ReuseWorkerEnvVariable, _reuseWorker);
        }
    }
}
