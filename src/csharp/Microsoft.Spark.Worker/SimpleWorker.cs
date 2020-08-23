// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Net;
using Microsoft.Spark.Network;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Worker
{
    internal sealed class SimpleWorker
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(SimpleWorker));

        private readonly Version _version;

        internal SimpleWorker(Version version)
        {
            _version = version;
        }

        internal void Run()
        {
            ISocketWrapper serverSocket = SocketFactory.CreateSocket();
            Run(serverSocket);
        }

        internal void Run(ISocketWrapper serverSocket)
        {
            try
            {
                string secret = Utils.SettingUtils.GetWorkerFactorySecret(_version);
                var ipEndpoint = (IPEndPoint)serverSocket.LocalEndPoint;
                int port = ipEndpoint.Port;

                ISocketWrapper clientSocket = SocketFactory.CreateSocket();

                s_logger.LogInfo($"RunSimpleWorker() is starting with port = {port}.");

                clientSocket.Connect(IPAddress.Loopback, port, secret);

                new TaskRunner(0, clientSocket, false, _version).Run();
            }
            catch (Exception e)
            {
                s_logger.LogError("RunSimpleWorker() failed with exception:");
                s_logger.LogException(e);
                Environment.Exit(-1);
            }

            s_logger.LogInfo("RunSimpleWorker() finished successfully");
        }
    }
}
