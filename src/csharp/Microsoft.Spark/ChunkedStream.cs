using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark
{
    /// <summary>
    /// This is a stream-like object that takes a stream of data, of unknown length, and breaks it
    /// into fixed length frames.The intended use case is serializing large data and sending it
    /// immediately over a socket -- we do not want to buffer the entire data before sending it,
    /// but the receiving end needs to know whether or not there is more data coming.
    /// It works by buffering the incoming data in some fixed-size chunks.  If the buffer is full,
    /// it first sends the buffer size, then the data. This repeats as long as there is more data
    /// to send. When this is closed, it sends the length of whatever data is in the buffer, then
    /// that data, and finally a "length" of -1 to indicate the stream has completed.
    /// </summary>
    public class ChunkedStream
    {
        private readonly int _bufferSize;
        private readonly byte[] _buffer;
        private int _currentPos;
        private readonly Stream _stream;

        internal ChunkedStream(Stream stream, int bufferSize)
        {
            _bufferSize = bufferSize;
            _buffer = new byte[_bufferSize];
            _currentPos = 0;
            _stream = stream;
        }

        /// <summary>
        /// Converts the given object value into array of bytes.
        /// </summary>
        /// <param name="value">Value of type object to convert to byte array.</param>
        /// <returns>Array of bytes</returns>
        internal byte[] ConvertToByteArray(object value)
        {
            var formatter = new BinaryFormatter();
            using var ms = new MemoryStream();
            formatter.Serialize(ms, value);
            return ms.ToArray();
        }

        /// <summary>
        /// Writes the value into the stream of type <see cref="Stream"/> in fixed chunks.
        /// </summary>
        /// <param name="value">Value of type object to write.</param>
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
                    SerDe.Write(_stream, _bufferSize);
                    _stream.Write(_buffer, 0, _bufferSize);
                    bytesRemaining -= spaceLeft;
                    bytePos = newBytePos;
                    _currentPos = 0;
                }
            }
        }

        /// <summary>
        /// Writes the remaining bytes left in _buffer and finishes it by writing -1 to the _stream
        /// and then closing it.
        /// </summary>
        public void Close()
        {
            // If there is anything left in the buffer, write it out first.
            if (_currentPos > 0)
            {
                SerDe.Write(_stream, _currentPos);
                _stream.Write(_buffer, 0, _currentPos);
            }
            // -1 length indicates to the receiving end that we're done.
            SerDe.Write(_stream, -1);
            _stream.Close();
        }
    }
}
