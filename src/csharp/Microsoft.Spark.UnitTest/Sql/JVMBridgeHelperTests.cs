// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Net;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Moq;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class JVMBridgeHelperTests
    {
        [Theory]
        [InlineData("1,2,3", 0, false)]
        [InlineData("5567,5568,5569", 0, true)]
        [InlineData("1,2,3", 5567, false)]
        [InlineData("5567,5568,5569", 5567, true)]
        [InlineData("1234,5678", 1234, true)]
        public void IsDotnetBackendPortUsingTest(string testRunningPorts, int backendport, bool expect)
        {
            var envkey = "DOTNETBACKEND_PORT";
            var oldenvValue = Environment.GetEnvironmentVariable(envkey);
            try
            {
                if (backendport == 0)
                {
                    Environment.SetEnvironmentVariable(envkey, null);
                }
                else
                {
                    Environment.SetEnvironmentVariable(envkey, backendport.ToString());
                }

                var runningports = testRunningPorts
                    .Split(",").Select(x => Convert.ToInt32(x)).ToList();
                var mockIpInfos = runningports
                    .Select(p =>
                    {
                        var m_info = new Mock<TcpConnectionInformation>();
                        m_info.SetupGet(m => m.LocalEndPoint).Returns(new IPEndPoint(0, p));
                        return m_info.Object;
                    }).ToArray();
                var ipinfo = new Mock<IPGlobalProperties>();
                ipinfo.Setup(m => m.GetActiveTcpConnections())
                    .Returns(mockIpInfos);

                var ret = JVMBridgeHelper.IsDotnetBackendPortUsing(ipinfo.Object);
                Assert.Equal(ret, expect);
            }
            finally
            {
                Environment.SetEnvironmentVariable(envkey, oldenvValue);
            }
        }

        /// <summary>
        /// The main test case of JVM bridge helper,
        /// is simply run it and close it.
        /// </summary>
        [Fact]
        public void JVMBridgeHelperMainPathTest() 
        {
            using(var helper = new JVMBridgeHelper()) {
                // now we should be able to connect to JVM bridge
                // or if system environment is not good, we should not failed.
            }
        }        

        /// <summary>
        /// Test with case that already have jvm Bridge case
        /// </summary>
        [Fact]
        public void JVMBridgeHelperTestsWithSparkSessionWithBridgeReady() 
        {
            using(var helper = new JVMBridgeHelper()) {
                // now we should be able to connect to JVM bridge
                var spark = SparkSession.Builder().Master("local").GetOrCreate();
                spark.Stop();
            }
        }

        [Fact]
        public void JVMBridgeHelperTestsWithSparkSessionWithBridgeNotReady() 
        {
            // now we should be able to connect to JVM bridge anytime
            var spark = SparkSession.Builder().Master("local").GetOrCreate();
            spark.Stop();
        }
    }
}