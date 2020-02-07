// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Razorvine.Pickle;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class TaskRunnerTests
    {
        [Fact]
        public void TestTaskRunner()
        {
            using var serverListener = new DefaultSocketWrapper();
            serverListener.Listen();

            var port = (serverListener.LocalEndPoint as IPEndPoint).Port;
            var clientSocket = new DefaultSocketWrapper();
            clientSocket.Connect(IPAddress.Loopback, port, null);

            PayloadWriter payloadWriter = new PayloadWriterFactory().Create();
            var taskRunner = new TaskRunner(0, clientSocket, false, payloadWriter.Version);
            Task clientTask = Task.Run(() => taskRunner.Run());

            using (ISocketWrapper serverSocket = serverListener.Accept())
            {
                System.IO.Stream inputStream = serverSocket.InputStream;
                System.IO.Stream outputStream = serverSocket.OutputStream;

                Payload payload = TestData.GetDefaultPayload();
                CommandPayload commandPayload = TestData.GetDefaultCommandPayload();

                payloadWriter.Write(outputStream, payload, commandPayload);

                // Write 10 rows to the output stream.
                var pickler = new Pickler();
                for (int i = 0; i < 10; ++i)
                {
                    byte[] pickled = pickler.dumps(
                        new[] { new object[] { i.ToString(), i, i } });
                    SerDe.Write(outputStream, pickled.Length);
                    SerDe.Write(outputStream, pickled);
                }

                // Signal the end of data and stream.
                SerDe.Write(outputStream, (int)SpecialLengths.END_OF_DATA_SECTION);
                SerDe.Write(outputStream, (int)SpecialLengths.END_OF_STREAM);
                outputStream.Flush();

                // Now process the bytes flowing in from the client.
                bool timingDataReceived = false;
                bool exceptionThrown = false;
                var rowsReceived = new List<object[]>();

                while (true)
                {
                    int length = SerDe.ReadInt32(inputStream);
                    if (length > 0)
                    {
                        byte[] pickledBytes = SerDe.ReadBytes(inputStream, length);
                        using var unpickler = new Unpickler();
                        var rows = unpickler.loads(pickledBytes) as ArrayList;
                        foreach (object row in rows)
                        {
                            rowsReceived.Add((object[])row);
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

                Assert.True(timingDataReceived);
                Assert.False(exceptionThrown);

                // Validate rows received.
                Assert.Equal(10, rowsReceived.Count);
                for (int i = 0; i < 10; ++i)
                {
                    // Two UDFs registered, thus expecting two columns.
                    // Refer to TestData.GetDefaultCommandPayload().
                    object[] row = rowsReceived[i];
                    Assert.Equal(2, rowsReceived[i].Length);
                    Assert.Equal($"udf2 udf1 {i}", row[0]);
                    Assert.Equal(i + i, row[1]);
                }
            }

            Assert.True(clientTask.Wait(5000));
        }
    }
}
