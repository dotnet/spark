using System.Collections;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Razorvine.Pickle;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    /// <summary>
    /// Payload reader that reads the output of the inputStream of the socket response  
    /// </summary>
    internal sealed class PayloadReader
    {
        public static List<object[]> Read(Stream inputStream)
        {
            bool timingDataReceived = false;
            bool exceptionThrown = false;
            var rowsReceived = new List<object[]>();
            
            while (true)
            {
                int length = SerDe.ReadInt32(inputStream);
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
            
            Assert.True(timingDataReceived);
            Assert.False(exceptionThrown);
            
            return rowsReceived;
        }

        public static int ReadInt(Stream inputStream)
        {
            int value = SerDe.ReadInt32(inputStream);
            return value;
        }
    }
}