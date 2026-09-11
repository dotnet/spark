// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Microsoft.Spark.Network;
using Microsoft.Spark.Services;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class SocketBufferingTests
    {
        [Theory]
        [InlineData(null)]
        [InlineData(true)]
        [InlineData(false)]
        public void TestPerSocketBufferingPreservesAcceptedSocketDefaults(bool? useBufferedStreams)
        {
            string readBufferName = ConfigurationService.WorkerReadBufferSizeEnvVarName;
            string writeBufferName = ConfigurationService.WorkerWriteBufferSizeEnvVarName;
            string readBuffer = Environment.GetEnvironmentVariable(readBufferName);
            string writeBuffer = Environment.GetEnvironmentVariable(writeBufferName);
            try
            {
                Environment.SetEnvironmentVariable(readBufferName, "32768");
                Environment.SetEnvironmentVariable(writeBufferName, "32768");

                using var listener = new DefaultSocketWrapper();
                listener.Listen();
                Func<ISocketWrapper> defaultFactory = SocketFactory.CreateSocket;
                using ISocketWrapper client = useBufferedStreams.HasValue ?
                    SocketFactory.CreateSocket(useBufferedStreams.Value) : defaultFactory();
                client.Connect(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndPoint).Port);
                using ISocketWrapper accepted = listener.Accept();

                if (useBufferedStreams ?? true)
                {
                    Assert.IsType<BufferedStream>(client.InputStream);
                    Assert.IsType<BufferedStream>(client.OutputStream);
                }
                else
                {
                    Assert.IsType<NetworkStream>(client.InputStream);
                    Assert.IsType<NetworkStream>(client.OutputStream);
                }

                Assert.IsType<BufferedStream>(accepted.InputStream);
                Assert.IsType<BufferedStream>(accepted.OutputStream);
            }
            finally
            {
                Environment.SetEnvironmentVariable(readBufferName, readBuffer);
                Environment.SetEnvironmentVariable(writeBufferName, writeBuffer);
            }
        }
    }
}
