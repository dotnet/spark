// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Microsoft.Spark.Worker.Processor
{
    /// <summary>
    /// Reads bounded, big-endian fields from a Worker protocol stream.
    /// </summary>
    internal sealed class ProtocolReader
    {
        private static readonly UTF8Encoding s_strictUtf8 =
            new UTF8Encoding(false, true);

        private readonly Stream _stream;
        private readonly byte[] _fixedWidthBuffer = new byte[sizeof(long)];

        internal ProtocolReader(Stream stream)
        {
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        internal bool ReadBoolean(string fieldName)
        {
            int value = _stream.ReadByte();
            if (value < 0)
            {
                throw new EndOfStreamException($"Incomplete {fieldName}.");
            }

            return value switch
            {
                0 => false,
                1 => true,
                _ => throw new InvalidDataException($"Invalid {fieldName}.")
            };
        }

        internal int ReadInt32(string fieldName)
        {
            ReadExactly(_fixedWidthBuffer, sizeof(int), fieldName);
            return BinaryPrimitives.ReadInt32BigEndian(
                _fixedWidthBuffer.AsSpan(0, sizeof(int)));
        }

        internal long ReadInt64(string fieldName)
        {
            ReadExactly(_fixedWidthBuffer, sizeof(long), fieldName);
            return BinaryPrimitives.ReadInt64BigEndian(_fixedWidthBuffer);
        }

        internal byte[] ReadBytes(int length, string fieldName)
        {
            var bytes = new byte[length];
            ReadExactly(bytes, length, fieldName);
            return bytes;
        }

        internal string ReadUtf8(
            string fieldName,
            int minimumLength,
            int maximumLength)
        {
            int length = ReadInt32($"{fieldName} length");
            if (length < minimumLength || length > maximumLength)
            {
                throw new InvalidDataException($"Invalid {fieldName} length.");
            }

            byte[] bytes = ReadBytes(length, fieldName);
            try
            {
                return s_strictUtf8.GetString(bytes);
            }
            catch (DecoderFallbackException ex)
            {
                throw new InvalidDataException($"Invalid {fieldName} encoding.", ex);
            }
        }

        private void ReadExactly(byte[] buffer, int length, string fieldName)
        {
            int offset = 0;
            while (offset < length)
            {
                int bytesRead = _stream.Read(buffer, offset, length - offset);
                if (bytesRead == 0)
                {
                    throw new EndOfStreamException($"Incomplete {fieldName}.");
                }

                offset += bytesRead;
            }
        }
    }
}
