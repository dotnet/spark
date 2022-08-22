// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.Net;
using Microsoft.Spark.Interop.Ipc;
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
            int port = Utils.SettingUtils.GetWorkerFactoryPort();
            Run(port);
        }

        internal void Run(int port)
        {
            try
            {
                string secret = Utils.SettingUtils.GetWorkerFactorySecret();

                s_logger.LogInfo($"RunSimpleWorker() is starting with port = {port}.");

                ISocketWrapper socket = SocketFactory.CreateSocket();
                socket.Connect(IPAddress.Loopback, port, secret);

                if ((_version.Major == 3 && _version.Minor >= 2) || _version.Major > 3)
                {
                    int pid = Process.GetCurrentProcess().Id;
                    SerDe.Write(socket.OutputStream, pid);
                    socket.OutputStream.Flush();
                }

                new TaskRunner(0, socket, false, _version).Run();
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
