using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark
{
    public class ChunkedStream
    {
        private readonly int _bufferSize;
        private byte[] _buffer;
        private int _currentPos;
        private Stream _wrapped;

        internal ChunkedStream(Stream wrapped, int bufferSize)
        {
            _bufferSize = bufferSize;
            _buffer = new byte[_bufferSize];
            _currentPos = 0;
            _wrapped = wrapped;
        }

        public void Write(byte[] bytes)
        {
            int bytePos = 0;
            int bytesRemaining = bytes.Length;
            while (bytesRemaining > 0)
            {
                int newPos = bytesRemaining + _currentPos;
                if (newPos < _bufferSize)
                {
                    Array.Copy(_buffer, _currentPos, bytes, bytePos, bytesRemaining);
                    _currentPos = newPos;
                    bytesRemaining = 0;
                }
                else
                {
                    // fill the buffer, send the length then the contents, and start filling again
                    int spaceLeft = _bufferSize - _currentPos;
                    int newBytePos = bytePos + spaceLeft;
                    Array.Copy(_buffer, _currentPos, bytes, bytePos, spaceLeft);
                }
            }
        }
    }
}
