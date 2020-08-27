using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Spark.Network;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class SimpleWorkerTests
    {
        [Fact]
        public void TestsSimpleWorkerTaskRunners()
        {
            var typedVersion = new Version(Versions.V2_4_0);
            var simpleWorker = new SimpleWorker(typedVersion);
            int port = Utils.SettingUtils.GetWorkerFactoryPort(typedVersion);
            ISocketWrapper serverListent = SocketFactory.CreateSocket();
            serverListent.Listen();

            PayloadWriter payloadWriter = new PayloadWriterFactory().Create();
            Task clientTask = Task.Run(() => simpleWorker.Run(serverListent, port));

            using (ISocketWrapper serverSocket = serverListent.Accept())
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
