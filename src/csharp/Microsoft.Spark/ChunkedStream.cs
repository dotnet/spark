using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
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

        internal void WriteInt(int value, Stream stream)
        {
            byte[] bytes = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            stream.Write(bytes, 0, bytes.Length);
        }

        internal byte[] ConvertToByteArray(object value)
        {
            var formatter = new BinaryFormatter();
            using (var ms = new MemoryStream())
            {
                formatter.Serialize(ms, value);
                return ms.ToArray();
            }
        }

        public void Write(object value)
        {
            byte[] bytes = ConvertToByteArray(value);
            int bytePos = 0;
            int bytesRemaining = bytes.Length;
            while (bytesRemaining > 0)
            {
                int newPos = bytesRemaining + _currentPos;
                if (newPos < _bufferSize)
                {
                    Array.Copy(bytes, bytePos, _buffer, _currentPos, bytesRemaining);
                    _currentPos = newPos;
                    bytesRemaining = 0;
                }
                else
                {
                    // Fill the buffer, send the length then the contents, and start filling again.
                    int spaceLeft = _bufferSize - _currentPos;
                    int newBytePos = bytePos + spaceLeft;
                    Array.Copy(bytes, bytePos, _buffer, _currentPos, spaceLeft);
                    WriteInt(_bufferSize, _wrapped);
                    _wrapped.Write(_buffer, 0, _bufferSize);
                    bytesRemaining -= spaceLeft;
                    bytePos = newBytePos;
                    _currentPos = 0;
                }
            }
        }

        public void Close()
        {
            // If there is anything left in the buffer, write it out first.
            if (_currentPos > 0)
            {
                WriteInt(_currentPos, _wrapped);
                _wrapped.Write(_buffer, 0, _currentPos + 1);
            }
            // -1 length indicates to the receiving end that we're done.
            WriteInt(-1, _wrapped);
            _wrapped.Close();
        }
    }
}
