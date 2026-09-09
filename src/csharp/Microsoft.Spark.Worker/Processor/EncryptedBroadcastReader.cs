// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.IO;
using MessagePack;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Worker.Processor
{
    /// <summary>
    /// Reads interleaved broadcast IDs and MessagePack values from the decryption socket.
    /// </summary>
    internal sealed class EncryptedBroadcastReader
    {
        private const int ReadChunkSize = 8192;
        // The runtime's maximum byte[] length; Array.MaxLength is unavailable on .NET Framework.
        private const int MaxByteArrayLength = 0x7FFFFFC7;
        private readonly Stream _stream;
        private byte[] _buffer = new byte[ReadChunkSize];
        private int _offset;
        private int _count;

        internal EncryptedBroadcastReader(Stream stream)
        {
            _stream = stream;
        }

        internal long ReadId()
        {
            while (_count < sizeof(long))
            {
                ReadMore();
            }

            long id = BinaryPrimitives.ReadInt64BigEndian(
                _buffer.AsSpan(_offset, sizeof(long)));
            Consume(sizeof(long));
            return id;
        }

        internal object ReadValue()
        {
            while (true)
            {
                // An incomplete Skip does not establish a consumed position. Retry from
                // the preserved object start after each refill, never from reader.Position.
                var reader = new MessagePackReader(
                    new ReadOnlyMemory<byte>(_buffer, _offset, _count));
                try
                {
                    reader.Skip();
                }
                catch (EndOfStreamException)
                {
                    ReadMore();
                    continue;
                }

                int length = checked((int)reader.Consumed);
                using var valueStream = new MemoryStream(
                    _buffer, _offset, length, writable: false);
                object value = BinarySerDe.Deserialize<object>(valueStream);
                Consume(length);
                return value;
            }
        }

        private void Consume(int length)
        {
            _offset += length;
            _count -= length;
        }

        internal static int GetExpandedCapacity(int currentCapacity)
        {
            if (currentCapacity >= MaxByteArrayLength)
            {
                throw new InvalidDataException(
                    "Encrypted broadcast exceeds the maximum supported byte-array length.");
            }

            return (int)Math.Min((long)currentCapacity * 2, MaxByteArrayLength);
        }

        private void ReadMore()
        {
            if (_offset != 0)
            {
                Buffer.BlockCopy(_buffer, _offset, _buffer, 0, _count);
                _offset = 0;
            }

            if (_count == _buffer.Length)
            {
                Array.Resize(ref _buffer, GetExpandedCapacity(_buffer.Length));
            }

            int count = _stream.Read(
                _buffer, _count, Math.Min(ReadChunkSize, _buffer.Length - _count));
            if (count == 0)
            {
                throw new EndOfStreamException("Truncated encrypted broadcast data.");
            }

            _count += count;
        }
    }
}
