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
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class CallbackTests
    {
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

            IOrderedEnumerable<int> actualIds = ids.OrderBy(i => i);
            IEnumerable<int> expectedIds = Enumerable.Range(1, numToRegister);
            Assert.True(expectedIds.SequenceEqual(actualIds));
        }

        [Fact]
        public void TestCallbackServer()
        {
            var callbackServer = new CallbackServer();
            var callbackHandler = new ReturnValueHandler();

            callbackHandler.Id = callbackServer.RegisterCallback(callbackHandler);
            Assert.Equal(1, callbackHandler.Id);

            using ISocketWrapper callbackSocket = SocketFactory.CreateSocket();
            callbackServer.Run(callbackSocket);

            int connectionNumber = 10;
            for (int i = 0; i < connectionNumber; ++i)
            {
                var ipEndpoint = (IPEndPoint)callbackSocket.LocalEndPoint;
                ISocketWrapper clientSocket = SocketFactory.CreateSocket();
                clientSocket.Connect(ipEndpoint.Address, ipEndpoint.Port);

                CreateAndVerifyConnection(clientSocket, callbackHandler, i);
            }

            Assert.Equal(connectionNumber, callbackServer.CurrentNumConnections);

            IOrderedEnumerable<int> actualValues = callbackHandler.Inputs.OrderBy(i => i);
            IEnumerable<int> expectedValues = Enumerable
                .Range(0, connectionNumber)
                .Select(i => callbackHandler.ApplyToInput(i));
            Assert.Equal(connectionNumber, callbackHandler.Inputs.Count);
            Assert.True(expectedValues.SequenceEqual(actualValues));
        }

        [Fact]
        public void TestCallbackHandlers()
        {
            var callbackHandlersDict = new ConcurrentDictionary<int, ICallbackHandler>();
            {
                // Test CallbackConnection using a ICallbackHandler that has a 
                // return value.
                var callbackHandler = new ReturnValueHandler
                {
                    Id = 1
                };
                callbackHandlersDict[callbackHandler.Id] = callbackHandler;
                TestCallbackConnection(callbackHandlersDict, callbackHandler);
            }
            {
                // Test CallbackConnection using a ICallbackHandler that does
                // not return a value.
                var callbackHandler = new NoReturnValueHandler
                {
                    Id = 2
                };
                callbackHandlersDict[callbackHandler.Id] = callbackHandler;
                TestCallbackConnection(callbackHandlersDict, callbackHandler);
            }
            {
                // Test CallbackConnection using a ICallbackHandler that does
                // not return a value.
                var callbackHandler = new ThrowsExceptionHandler
                {
                    Id = 3
                };
                callbackHandlersDict[callbackHandler.Id] = callbackHandler;
                TestCallbackConnection(callbackHandlersDict, callbackHandler);
            }
        }

        private void TestCallbackConnection(
            ConcurrentDictionary<int, ICallbackHandler> callbackHandlersDict,
            ITestCallbackHandler callbackHandler)
        {
            using ISocketWrapper serverListener = SocketFactory.CreateSocket();
            serverListener.Listen();

            var ipEndpoint = (IPEndPoint)serverListener.LocalEndPoint;
            ISocketWrapper clientSocket = SocketFactory.CreateSocket();
            clientSocket.Connect(ipEndpoint.Address, ipEndpoint.Port);

            var cancellationToken = new CancellationToken();
            var callbackConnection = new CallbackConnection(0, clientSocket, callbackHandlersDict);
            Task connectionTask = Task.Run(() => callbackConnection.Run(cancellationToken));

            using ISocketWrapper serverSocket = serverListener.Accept();
            CreateAndVerifyConnection(serverSocket, callbackHandler, 1);
        }

        private void CreateAndVerifyConnection(
            ISocketWrapper socket,
            ITestCallbackHandler callbackHandler,
            int inputToHandler)
        {
            int callbackReturnValue = WriteAndReadTestData(
                socket.InputStream,
                socket.OutputStream,
                callbackHandler,
                inputToHandler);

            if (callbackHandler.HasReturnValue && !callbackHandler.Throws)
            {
                Assert.Equal(callbackHandler.ApplyToOutput(inputToHandler), callbackReturnValue);
            }
            else
            {
                Assert.Equal(int.MinValue, callbackReturnValue);
            }
        }

        private int WriteAndReadTestData(
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
            if (callbackHandler.HasReturnValue &&
                TryGetCallbackFlag(inputStream, out int returnFlag))
            {
                Assert.Equal(
                    (int)CallbackFlags.CALLBACK_RETURN_VALUE, returnFlag);
                callbackReturnValue = SerDe.ReadInt32(inputStream);
            }

            SerDe.Write(outputStream, (int)CallbackFlags.END_OF_STREAM);
            outputStream.Flush();

            if (TryGetCallbackFlag(inputStream, out int endOfStreamFlag))
            {
                Assert.Equal((int)CallbackFlags.END_OF_STREAM, endOfStreamFlag);
            }

            return callbackReturnValue;
        }

        private bool TryGetCallbackFlag(Stream inputStream, out int callbackFlag)
        {
            callbackFlag = SerDe.ReadInt32(inputStream);
            if (callbackFlag == (int)CallbackFlags.DOTNET_EXCEPTION_THROWN)
            {
                string exceptionMessage = SerDe.ReadString(inputStream);
                Assert.True(!string.IsNullOrEmpty(exceptionMessage));
                Assert.Contains("ThrowsCallbackHandler", exceptionMessage);
                return false;
            }

            return true;
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

            public bool Throws { get; } = false;

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

            public bool Throws { get; } = false;

            public int ApplyToInput(int i) => 10 * i;

            public int ApplyToOutput(int i) => throw new NotImplementedException();
        }

        private class ThrowsExceptionHandler : ICallbackHandler, ITestCallbackHandler
        {
            public void Run(Stream inputStream, Stream outputStream)
            {
                throw new Exception("ThrowsCallbackHandler");
            }

            public ConcurrentBag<int> Inputs { get; } = new ConcurrentBag<int>();

            public int Id { get; set; }

            public bool HasReturnValue { get; } = false;

            public bool Throws { get; } = true;

            public int ApplyToInput(int i) => throw new NotImplementedException();

            public int ApplyToOutput(int i) => throw new NotImplementedException();
        }

        private interface ITestCallbackHandler
        {
            ConcurrentBag<int> Inputs { get; }

            int Id { get; set; }

            bool HasReturnValue { get; }

            bool Throws { get; }

            int ApplyToInput(int i);

            int ApplyToOutput(int i);
        }
    }
}
