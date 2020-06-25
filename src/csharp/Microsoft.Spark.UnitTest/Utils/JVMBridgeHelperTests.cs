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
        public void IsDotnetBackendPortUsingTest(string testListeningPorts, int backendport, bool expect)
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

                var listeningEndpoints = testListeningPorts
                    .Split(",").Select(x => new IPEndPoint(0, Convert.ToInt32(x))).ToArray();

                var ipinfo = new Mock<IPGlobalProperties>();
                ipinfo.Setup(m => m.GetActiveTcpListeners())
                    .Returns(listeningEndpoints);

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
            using (var helper = new JVMBridgeHelper())
            {
                // now we should be able to connect to JVM bridge
                // or if system environment is not good, we should not failed.
            }
        }

        [Fact]
        public void JVMBridgeHelperWithoutSpark()
        {
            var oldhome = Environment.GetEnvironmentVariable("SPARK_HOME");
            Environment.SetEnvironmentVariable("SPARK_HOME", null);
            using (var helper = new JVMBridgeHelper())
            {
            }
            Environment.SetEnvironmentVariable("SPARK_HOME", oldhome);
        }
    }
}