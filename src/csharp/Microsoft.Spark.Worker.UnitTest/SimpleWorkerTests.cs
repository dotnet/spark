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
            using ISocketWrapper serverListener = SocketFactory.CreateSocket();
            var ipEndpoint = (IPEndPoint)serverListener.LocalEndPoint;

            serverListener.Listen();

            var typedVersion = new Version(version);
            var simpleWorker = new SimpleWorker(typedVersion);

            Task clientTask = Task.Run(() => simpleWorker.Run(ipEndpoint.Port));

            PayloadWriter payloadWriter = new PayloadWriterFactory().Create(typedVersion);
            TaskRunnerTests.TestTaskRunnerReadWrite(serverListener, payloadWriter);

            Assert.True(clientTask.Wait(5000));
        }
    }
}
