// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Moq;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class CallbackTests
    {
        private readonly Mock<IJvmBridge> _mockJvm;

        public CallbackTests(SparkFixture fixture)
        {
            _mockJvm = fixture.MockJvm;
        }

        [Fact]
        public async Task TestCallbackIds()
        {
            int numToRegister = 100;
            var callbackServer = new CallbackServer(_mockJvm.Object, false);
            var callbackHandler = new TestCallbackHandler();

            var ids = new ConcurrentBag<int>();
            var tasks = new List<Task>();
            for (int i = 0; i < numToRegister; ++i)
            {
                tasks.Add(
                    Task.Run(() => ids.Add(callbackServer.RegisterCallback(callbackHandler))));
            }

            await Task.WhenAll(tasks);

            IOrderedEnumerable<int> actualIds = ids.OrderBy(i => i);
            IEnumerable<int> expectedIds = Enumerable.Range(1, numToRegister);
            Assert.True(expectedIds.SequenceEqual(actualIds));
        }

        [Fact]
        public void TestCallbackServer()
        {
            var callbackServer = new CallbackServer(_mockJvm.Object, false);
            var callbackHandler = new TestCallbackHandler();

            callbackHandler.Id = callbackServer.RegisterCallback(callbackHandler);
            Assert.Equal(1, callbackHandler.Id);

            using ISocketWrapper callbackSocket = SocketFactory.CreateSocket();
            callbackServer.Run(callbackSocket);

            int connectionNumber = 2;
            var clientSockets = new ISocketWrapper[connectionNumber];
            for (int i = 0; i < connectionNumber; ++i)
            {
                var ipEndpoint = (IPEndPoint)callbackSocket.LocalEndPoint;
                ISocketWrapper clientSocket = SocketFactory.CreateSocket();
                clientSockets[i] = clientSocket;
                clientSocket.Connect(ipEndpoint.Address, ipEndpoint.Port);

                WriteAndReadTestData(clientSocket, callbackHandler, i);
            }

            Assert.Equal(connectionNumber, callbackServer.CurrentNumConnections);

            IOrderedEnumerable<int> actualValues = callbackHandler.Inputs.OrderBy(i => i);
            IEnumerable<int> expectedValues = Enumerable
                .Range(0, connectionNumber)
                .Select(i => callbackHandler.Apply(i))
                .OrderBy(i => i);
            Assert.True(expectedValues.SequenceEqual(actualValues));
        }

        [Fact]
        public void TestCallbackHandlers()
        {
            var tokenSource = new CancellationTokenSource();
            var callbackHandlersDict = new ConcurrentDictionary<int, ICallbackHandler>();
            int inputToHandler = 1;
            {
                // Test CallbackConnection using a ICallbackHandler that runs
                // normally without error.
                var callbackHandler = new TestCallbackHandler
                {
                    Id = 1
                };
                callbackHandlersDict[callbackHandler.Id] = callbackHandler;
                TestCallbackConnection(
                    callbackHandlersDict,
                    callbackHandler,
                    inputToHandler,
                    tokenSource.Token);
                Assert.Single(callbackHandler.Inputs);
                Assert.Equal(
                    callbackHandler.Apply(inputToHandler),
                    callbackHandler.Inputs.First());
            }
            {
                // Test CallbackConnection using a ICallbackHandler that 
                // throws an exception.
                var callbackHandler = new ThrowsExceptionHandler
                {
                    Id = 2
                };
                callbackHandlersDict[callbackHandler.Id] = callbackHandler;
                TestCallbackConnection(
                    callbackHandlersDict,
                    callbackHandler,
                    inputToHandler,
                    tokenSource.Token);
                Assert.Empty(callbackHandler.Inputs);
            }
            {
                // Test CallbackConnection when cancellation has been requested for the token.
                tokenSource.Cancel();
                var callbackHandler = new TestCallbackHandler
                {
                    Id = 3
                };
                callbackHandlersDict[callbackHandler.Id] = callbackHandler;
                TestCallbackConnection(
                    callbackHandlersDict,
                    callbackHandler,
                    inputToHandler,
                    tokenSource.Token);
                Assert.Empty(callbackHandler.Inputs);
            }
        }
        
        [Fact]
        public void TestJvmCallbackClientProperty()
        {
            var server = new CallbackServer(_mockJvm.Object, run: false);
            Assert.Throws<InvalidOperationException>(() => server.JvmCallbackClient);
            
            using ISocketWrapper callbackSocket = SocketFactory.CreateSocket();
            server.Run(callbackSocket); 
            Assert.NotNull(server.JvmCallbackClient);
        }

        private void TestCallbackConnection(
            ConcurrentDictionary<int, ICallbackHandler> callbackHandlersDict,
            ITestCallbackHandler callbackHandler,
            int inputToHandler,
            CancellationToken token)
        {
            using ISocketWrapper serverListener = SocketFactory.CreateSocket();
            serverListener.Listen();

            var ipEndpoint = (IPEndPoint)serverListener.LocalEndPoint;
            using ISocketWrapper clientSocket = SocketFactory.CreateSocket();
            clientSocket.Connect(ipEndpoint.Address, ipEndpoint.Port);

            // Don't use "using" here. The CallbackConnection will dispose the socket.
            ISocketWrapper serverSocket = serverListener.Accept();
            var callbackConnection = new CallbackConnection(0, serverSocket, callbackHandlersDict);
            Task task = Task.Run(() => callbackConnection.Run(token));

            if (token.IsCancellationRequested)
            {
                task.Wait();
                Assert.False(callbackConnection.IsRunning);
            }
            else
            {
                WriteAndReadTestData(clientSocket, callbackHandler, inputToHandler);

                if (callbackHandler.Throws)
                {
                    task.Wait();
                    Assert.False(callbackConnection.IsRunning);
                }
                else
                {
                    Assert.True(callbackConnection.IsRunning);

                    // Clean up CallbackConnection
                    Stream outputStream = clientSocket.OutputStream;
                    SerDe.Write(outputStream, (int)CallbackConnection.ConnectionStatus.REQUEST_CLOSE);
                    outputStream.Flush();
                    task.Wait();
                    Assert.False(callbackConnection.IsRunning);
                }
            }
        }

        private void WriteAndReadTestData(
            ISocketWrapper socket,
            ITestCallbackHandler callbackHandler,
            int inputToHandler)
        {
            Stream inputStream = socket.InputStream;
            Stream outputStream = socket.OutputStream;

            SerDe.Write(outputStream, (int)CallbackFlags.CALLBACK);
            SerDe.Write(outputStream, callbackHandler.Id);
            SerDe.Write(outputStream, sizeof(int));
            SerDe.Write(outputStream, inputToHandler);
            SerDe.Write(outputStream, (int)CallbackFlags.END_OF_STREAM);
            outputStream.Flush();

            int callbackFlag = SerDe.ReadInt32(inputStream);
            if (callbackFlag == (int)CallbackFlags.DOTNET_EXCEPTION_THROWN)
            {
                string exceptionMessage = SerDe.ReadString(inputStream);
                Assert.False(string.IsNullOrEmpty(exceptionMessage));
                Assert.Contains(callbackHandler.ExceptionMessage, exceptionMessage);
            }
            else
            {
                Assert.Equal((int)CallbackFlags.END_OF_STREAM, callbackFlag);
            }
        }

        private class TestCallbackHandler : ICallbackHandler, ITestCallbackHandler
        {
            public void Run(Stream inputStream) => Inputs.Add(Apply(SerDe.ReadInt32(inputStream)));

            public ConcurrentBag<int> Inputs { get; } = new ConcurrentBag<int>();

            public int Id { get; set; }

            public bool Throws { get; } = false;

            public string ExceptionMessage => throw new NotImplementedException();

            public int Apply(int i) => 10 * i;
        }

        private class ThrowsExceptionHandler : ICallbackHandler, ITestCallbackHandler
        {
            public void Run(Stream inputStream) => throw new Exception(ExceptionMessage);

            public ConcurrentBag<int> Inputs { get; } = new ConcurrentBag<int>();

            public int Id { get; set; }

            public bool Throws { get; } = true;

            public string ExceptionMessage { get; } = "Dotnet Callback Handler Exception Message";

            public int Apply(int i) => throw new NotImplementedException();
        }

        private interface ITestCallbackHandler
        {
            ConcurrentBag<int> Inputs { get; }

            int Id { get; set; }

            bool Throws { get; }

            string ExceptionMessage { get; }

            int Apply(int i);
        }
    }
}
