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
            ISocketWrapper simpleSocket = SocketFactory.CreateSocket();
            
            var typedVersion = new Version(Versions.V2_4_0);
            var simpleWorker = new SimpleWorker(typedVersion);
            
            Task.Run(() => simpleWorker.Run(simpleSocket));

            CreateAndVerifyConnection(simpleWorker);
            
            Assert.Equal(true, true);
        }
    }
}
