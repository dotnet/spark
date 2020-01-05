// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Razorvine.Pickle;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class DaemonWorkerTests
    {
        [Fact]
        public void TestsDaemonWorkerTaskRunners()
        {
            ISocketWrapper daemonSocket = SocketFactory.CreateSocket();
            
            var taskRunnerNumber = 3;
            var typedVersion = new Version(Versions.V2_4_0);
            var daemonWorker = new DaemonWorker(typedVersion);
            
            Task.Run(() => daemonWorker.Run(daemonSocket));

            for (var i = 0; i < taskRunnerNumber; ++i)
            {
                CreateAndVerifyConnection(daemonSocket);
            }
            
            Assert.Equal(taskRunnerNumber, daemonWorker.CurrentNumTaskRunners);
        }

        private static void CreateAndVerifyConnection(ISocketWrapper daemonSocket)
        {
            var ipEndpoint = (IPEndPoint)daemonSocket.LocalEndPoint;
            int port = ipEndpoint.Port;
            ISocketWrapper clientSocket = SocketFactory.CreateSocket();
            clientSocket.Connect(ipEndpoint.Address, port);

            // Now process the bytes flowing in from the client.
            var timingDataReceived = false;
            var exceptionThrown = false;
            var rowsReceived = new List<object[]>();

            WriteTestData(clientSocket);
            ReadSocketResponse(
                clientSocket.InputStream, 
                rowsReceived, 
                ref timingDataReceived, 
                ref exceptionThrown);
            
            Assert.True(timingDataReceived);
            Assert.False(exceptionThrown);

            // Validate rows received.
            Assert.Equal(10, rowsReceived.Count);
            
            for (int i = 0; i < 10; ++i)
            {
                // Two UDFs registered, thus expecting two columns.
                // Refer to TestData.GetDefaultCommandPayload().
                var row = rowsReceived[i];
                Assert.Equal(2, rowsReceived[i].Length);
                Assert.Equal($"udf2 udf1 {i}", row[0]);
                Assert.Equal(i + i, row[1]);
            }
        }
        
        private static void WriteTestData(ISocketWrapper clientSocket)
        {
            PayloadWriter payloadWriter = new PayloadWriterFactory().Create();
            Stream outputStream = clientSocket.OutputStream;

            Payload payload = TestData.GetDefaultPayload();
            CommandPayload commandPayload = TestData.GetDefaultCommandPayload();

            payloadWriter.Write(outputStream, payload, commandPayload);

            // Write 10 rows to the output stream.
            var pickler = new Pickler();
            for (int i = 0; i < 10; ++i)
            {
                byte[] pickled = pickler.dumps(new[] { new object[] { i.ToString(), i, i } });
                SerDe.Write(outputStream, pickled.Length);
                SerDe.Write(outputStream, pickled);
            }

            // Signal the end of data and stream.
            SerDe.Write(outputStream, (int)SpecialLengths.END_OF_DATA_SECTION);
            SerDe.Write(outputStream, (int)SpecialLengths.END_OF_STREAM);
            outputStream.Flush();
        }

        private static void ReadSocketResponse(
            Stream inputStream, 
            List<object[]> rowsReceived, 
            ref bool timingDataReceived,
            ref bool exceptionThrown)
        {
            while (true)
            {
                var length = SerDe.ReadInt32(inputStream);
                if (length > 0)
                {
                    byte[] pickledBytes = SerDe.ReadBytes(inputStream, length);
                    var unpickler = new Unpickler();

                    var rows = unpickler.loads(pickledBytes) as ArrayList;
                    foreach (object row in rows)
                    {
                        rowsReceived.Add((object[]) row);
                    }
                }
                else if (length == (int)SpecialLengths.TIMING_DATA)
                {
                    long bootTime = SerDe.ReadInt64(inputStream);
                    long initTime = SerDe.ReadInt64(inputStream);
                    long finishTime = SerDe.ReadInt64(inputStream);
                    long memoryBytesSpilled = SerDe.ReadInt64(inputStream);
                    long diskBytesSpilled = SerDe.ReadInt64(inputStream);
                    timingDataReceived = true;
                }
                else if (length == (int)SpecialLengths.PYTHON_EXCEPTION_THROWN)
                {
                    SerDe.ReadString(inputStream);
                    exceptionThrown = true;
                    break;
                }
                else if (length == (int)SpecialLengths.END_OF_DATA_SECTION)
                {
                    int numAccumulatorUpdates = SerDe.ReadInt32(inputStream);
                    SerDe.ReadInt32(inputStream);
                    break;
                }
            }
        }
    }
}