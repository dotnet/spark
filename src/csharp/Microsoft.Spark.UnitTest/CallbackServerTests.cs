// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class CallbackServerTests
    {
        [Fact]
        public void TestCallbackHandlers()
        {
            {
                // Test CallbackServer using a ICallbackHandler that has a 
                // return value.
                var callbackServer = new CallbackServer();
                var callbackHandler = new ReturnValueHandler();
                TestCallbackServer(callbackServer, callbackHandler);
            }
            {
                // Test CallbackServer using a ICallbackHandler that does
                // not return a value.
                var callbackServer = new CallbackServer();
                var callbackHandler = new NoReturnValueHandler();
                TestCallbackServer(callbackServer, callbackHandler);
            }
        }

        [Fact]
        public async Task TestCallbackIds()
        {
            int numToRegister = 100;
            var callbackServer = new CallbackServer();
            var callbackHandler = new NoReturnValueHandler();

            var ids = new ConcurrentBag<int>();
            var tasks = new List<Task>();
            for (int i = 0; i < numToRegister; ++i)
            {
                tasks.Add(
                    Task.Run(() => ids.Add(callbackServer.RegisterCallback(callbackHandler))));
            }

            await Task.WhenAll(tasks);

            int[] actualIds = ids.OrderBy(i => i).ToArray();
            int[] expectedIds = Enumerable.Range(1, numToRegister).ToArray();
            Assert.True(expectedIds.SequenceEqual(actualIds));
        }

        private void TestCallbackServer(CallbackServer callbackServer, ITestCallbackHandler callbackHandler)
        {
            ISocketWrapper callbackSocket = SocketFactory.CreateSocket();

            int connectionNumber = 10;

            callbackHandler.Id =
                callbackServer.RegisterCallback((ICallbackHandler)callbackHandler);
            Assert.Equal(1, callbackHandler.Id);

            callbackServer.Run(callbackSocket);

            for (int i = 0; i < connectionNumber; ++i)
            {
                CreateAndVerifyConnection(callbackSocket, callbackHandler, i);
            }

            Assert.Equal(connectionNumber, callbackServer.CurrentNumConnections);

            int[] actualValues = callbackHandler.Inputs.OrderBy(i => i).ToArray();
            int[] expectedValues = Enumerable
                .Range(0, connectionNumber)
                .Select(i => callbackHandler.ApplyToInput(i))
                .ToArray();
            Assert.Equal(connectionNumber, callbackHandler.Inputs.Count);
            Assert.True(expectedValues.SequenceEqual(actualValues));
        }

        private static void CreateAndVerifyConnection(
            ISocketWrapper socket,
            ITestCallbackHandler callbackHandler,
            int inputToHandler)
        {
            var ipEndpoint = (IPEndPoint)socket.LocalEndPoint;
            int port = ipEndpoint.Port;
            ISocketWrapper clientSocket = SocketFactory.CreateSocket();
            clientSocket.Connect(ipEndpoint.Address, port);

            int callbackReturnValue = WriteAndReadTestData(
                clientSocket.InputStream,
                clientSocket.OutputStream,
                callbackHandler,
                inputToHandler);

            if (callbackHandler.HasReturnValue)
            {
                Assert.Equal(callbackHandler.ApplyToOutput(inputToHandler), callbackReturnValue);
            }
            else
            {
                Assert.Equal(int.MinValue, callbackReturnValue);
            }
        }

        private static int WriteAndReadTestData(
            Stream inputStream,
            Stream outputStream,
            ITestCallbackHandler callbackHandler,
            int inputToHandler)
        {
            SerDe.Write(outputStream, (int)CallbackFlags.CALLBACK);
            SerDe.Write(outputStream, callbackHandler.Id);
            SerDe.Write(outputStream, inputToHandler);
            outputStream.Flush();

            int callbackReturnValue = int.MinValue;
            if (callbackHandler.HasReturnValue)
            {
                Assert.Equal((int)CallbackFlags.CALLBACK_RETURN_VALUE, SerDe.ReadInt32(inputStream));
                callbackReturnValue = SerDe.ReadInt32(inputStream);
            }

            SerDe.Write(outputStream, (int)CallbackFlags.END_OF_STREAM);
            outputStream.Flush();
            Assert.Equal((int)CallbackFlags.END_OF_STREAM, SerDe.ReadInt32(inputStream));

            return callbackReturnValue;
        }

        private class ReturnValueHandler : ICallbackHandler, ITestCallbackHandler
        {
            public void Run(Stream inputStream, Stream outputStream)
            {
                int i = SerDe.ReadInt32(inputStream);
                Inputs.Add(ApplyToInput(i));
                SerDe.Write(outputStream, ApplyToOutput(i));
            }

            public ConcurrentBag<int> Inputs { get; } = new ConcurrentBag<int>();

            public int Id { get; set; }

            public bool HasReturnValue { get; } = true;

            public int ApplyToInput(int i) => 2 * i;

            public int ApplyToOutput(int i) => 3 * i;
        }

        private class NoReturnValueHandler : ICallbackHandler, ITestCallbackHandler
        {
            public void Run(Stream inputStream, Stream outputStream)
            {
                int i = SerDe.ReadInt32(inputStream);
                Inputs.Add(ApplyToInput(i));
            }

            public ConcurrentBag<int> Inputs { get; } = new ConcurrentBag<int>();

            public int Id { get; set; }

            public bool HasReturnValue { get; } = false;

            public int ApplyToInput(int i) => 10 * i;

            public int ApplyToOutput(int i) => throw new NotImplementedException();
        }

        private interface ITestCallbackHandler
        {
            ConcurrentBag<int> Inputs { get; }

            int Id { get; set; }

            bool HasReturnValue { get; }

            int ApplyToInput(int i);

            int ApplyToOutput(int i);
        }
    }
}
