using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.BridgeTests
{
    [Collection("Spark E2E Tests")]
    public class JvmBridgeTests
    {
        [Fact]
        public void TestSparkExceptionReturned()
        {
            using var session = SparkSession.GetDefaultSession();
            try
            {
                session.Sql("THROW!!!");
            }
            catch (Exception sparkEx)
            {
                Assert.NotNull(sparkEx.InnerException);
            }
        }
    }
}
