using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark
{
    public class ChunkedStream
    {
        private readonly int _bufferSize;
        private byte[] _buffer;
        //private int _currentPos;
        //private Stream _wrapped;

        internal ChunkedStream(Stream wrapped, int bufferSize)
        {
            _bufferSize = bufferSize;
            _buffer = new byte[_bufferSize];
            Console.WriteLine("here");
        }
    }
}
