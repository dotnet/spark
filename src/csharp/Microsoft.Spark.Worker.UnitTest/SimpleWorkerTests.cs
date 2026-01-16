using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Spark.Interop.Ipc;
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
            using ISocketWrapper serverListener = SocketFactory.CreateSocket();
            var ipEndpoint = (IPEndPoint)serverListener.LocalEndPoint;

            serverListener.Listen();

            var typedVersion = new Version(version);
            var simpleWorker = new SimpleWorker(typedVersion);

            Task clientTask = Task.Run(() => simpleWorker.Run(ipEndpoint.Port));

            PayloadWriter payloadWriter = new PayloadWriterFactory().Create(typedVersion);
            using (ISocketWrapper serverSocket = serverListener.Accept())
            {
                if ((typedVersion.Major == 3 && typedVersion.Minor >= 2) || typedVersion.Major > 3)
                {
                    int pid = SerDe.ReadInt32(serverSocket.InputStream);
                }

                TaskRunnerTests.TestTaskRunnerReadWrite(serverSocket, payloadWriter);
            }

            Assert.True(clientTask.Wait(5000));
        }

        [Theory]
        [InlineData("0", false)]
        [InlineData("1", true)]
        [InlineData(null, false)]
        [InlineData("", false)]
        [InlineData("true", false)]
        public void TestSparkReuseWorkerEnvironmentVariable(string envValue, bool expectedReuseWorker)
        {
            // Arrange
            string originalValue = Environment.GetEnvironmentVariable("SPARK_REUSE_WORKER");

            try
            {
                Environment.SetEnvironmentVariable("SPARK_REUSE_WORKER", envValue);

                // Act
                bool reuseWorker = "1".Equals(
                    Environment.GetEnvironmentVariable("SPARK_REUSE_WORKER"));

                // Assert
                Assert.Equal(expectedReuseWorker, reuseWorker);
            }
            finally
            {
                // Cleanup - restore original value
                Environment.SetEnvironmentVariable("SPARK_REUSE_WORKER", originalValue);
            }
        }
    }
}
