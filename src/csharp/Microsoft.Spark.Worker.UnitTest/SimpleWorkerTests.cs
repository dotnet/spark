using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Spark.Network;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class SimpleWorkerTests
    {
        [Theory]
        [MemberData(nameof(TestData.VersionData), MemberType = typeof(TestData))]
        public void TestsSimpleWorkerTaskRunners(string version)
        {
            var typedVersion = new Version(version);
            var simpleWorker = new SimpleWorker(typedVersion);
            using ISocketWrapper serverListen = SocketFactory.CreateSocket();
            var ipEndpoint = (IPEndPoint)serverListen.LocalEndPoint;
            int port = ipEndpoint.Port;

            serverListen.Listen();

            PayloadWriter payloadWriter = new PayloadWriterFactory().Create();
            Task clientTask = Task.Run(() => simpleWorker.Run(port));

            using (ISocketWrapper serverSocket = serverListen.Accept())
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

            Assert.True(clientTask.Wait(5000));
        }
    }
}
