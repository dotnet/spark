// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class CallbackServerTests
    {
        [Fact]
        public void TestsCallbackServerConnections()
        {
            ISocketWrapper callbackSocket = SocketFactory.CreateSocket();

            int connectionNumber = 10;
            var callbackServer = new CallbackServer();

            int callbackId = callbackServer.RegisterCallback(new CallbackHandler());
            Assert.Equal(1, callbackId);

            callbackServer.Run(callbackSocket);

            for (int i = 0; i < connectionNumber; ++i)
            {
                CreateAndVerifyConnection(callbackSocket, i);
            }

            Assert.Equal(connectionNumber, callbackServer.CurrentNumConnections);

            // [0, 1, 2, ..., connectionNumber - 1]
            int[] actualValues = CallbackHandler.ConcurrentBag.OrderBy(i => i).ToArray();
            Assert.Equal(connectionNumber, CallbackHandler.ConcurrentBag.Count);
            Assert.True(Enumerable.Range(0, connectionNumber).SequenceEqual(actualValues));
        }

        private static void CreateAndVerifyConnection(ISocketWrapper socket, int i)
        {
            var ipEndpoint = (IPEndPoint)socket.LocalEndPoint;
            int port = ipEndpoint.Port;
            ISocketWrapper clientSocket = SocketFactory.CreateSocket();
            clientSocket.Connect(ipEndpoint.Address, port);

            int callbackReturnValue = 
                WriteAndReadTestData(clientSocket.InputStream, clientSocket.OutputStream, i);

            Assert.Equal(CallbackHandler.Double(i), callbackReturnValue);
        }

        internal static int WriteAndReadTestData(Stream inputStream, Stream outputStream, int i)
        {
            SerDe.Write(outputStream, (int)CallbackFlags.CALLBACK);
            SerDe.Write(outputStream, 1);
            SerDe.Write(outputStream, i);
            outputStream.Flush();

            Assert.Equal((int)CallbackFlags.CALLBACK_RETURN_VALUE, SerDe.ReadInt32(inputStream));
            int callbackReturnValue = SerDe.ReadInt32(inputStream);

            SerDe.Write(outputStream, (int)CallbackFlags.END_OF_STREAM);
            outputStream.Flush();
            Assert.Equal((int)CallbackFlags.END_OF_STREAM, SerDe.ReadInt32(inputStream));

            return callbackReturnValue;
        }

        private class CallbackHandler : ICallbackHandler
        {
            public void Run(Stream inputStream, Stream outputStream)
            {
                int i = SerDe.ReadInt32(inputStream);
                ConcurrentBag.Add(i);
                SerDe.Write(outputStream, Double(i));
            }

            internal static ConcurrentBag<int> ConcurrentBag { get; } = new ConcurrentBag<int>();

            internal static int Double(int i) => i + i;
        }
    }
}
