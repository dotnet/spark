// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// Enums with which Worker communicates with Spark.
    /// See spark/core/src/main/scala/org/apache/spark/api/python/PythonRunner.scala.
    /// </summary>
    internal enum SpecialLengths : int
    {
        /// <summary>
        /// Flag to indicate the end of data section
        /// </summary>
        END_OF_DATA_SECTION = -1,

        /// <summary>
        /// Flag to indicate an exception thrown from .NET side
        /// </summary>
        PYTHON_EXCEPTION_THROWN = -2,

        /// <summary>
        /// Flag to indicate a timing data
        /// </summary>
        TIMING_DATA = -3,

        /// <summary>
        /// Flag to indicate the end of stream
        /// </summary>
        END_OF_STREAM = -4,

        /// <summary>
        /// Flag to indicate non-defined type
        /// </summary>
        NULL = -5,

        /// <summary>
        /// Flag used by PySpark only.
        /// </summary>
        START_ARROW_STREAM = -6
    }

    // TODO: When targeting .NET Core 2.1+ or .NET Standard 2.1+, all of this code can be simplified
    // using stackalloc'd spans and Stream.Read/Write(span).

    /// <summary>
    /// Serialization and Deserialization of data types between JVM and CLR
    /// </summary>
    internal class SerDe
    {
        [ThreadStatic]
        private static byte[] s_threadLocalBuffer;

        /// <summary>
        /// Reads a boolean from a stream.
        /// </summary>
        /// <param name="s">The stream to read</param>
        /// <returns>The boolean value read from the stream</returns>
        public static bool ReadBool(Stream s) =>
            Convert.ToBoolean(s.ReadByte());

        /// <summary>
        /// Reads an integer from a stream.
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The integer read from stream</returns>
        public static int ReadInt32(Stream s)
        {
            byte[] buffer = GetThreadLocalBuffer(sizeof(int));
            TryReadBytes(s, buffer, sizeof(int));
            return BinaryPrimitives.ReadInt32BigEndian(buffer);
        }

        /// <summary>
        /// Reads a long integer from a stream.
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The long integer read from stream</returns>
        public static long ReadInt64(Stream s)
        {
            byte[] buffer = GetThreadLocalBuffer(sizeof(long));
            TryReadBytes(s, buffer, sizeof(long));
            return BinaryPrimitives.ReadInt64BigEndian(buffer);
        }

        /// <summary>
        /// Reads a double from a stream.
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The double read from stream</returns>
        public static double ReadDouble(Stream s)
        {
            byte[] buffer = GetThreadLocalBuffer(sizeof(long));
            TryReadBytes(s, buffer, sizeof(long));
            return BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64BigEndian(buffer));
        }

        /// <summary>
        /// Reads a string from a stream
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The string read from stream</returns>
        public static string ReadString(Stream s)
        {
            byte[] buffer = GetThreadLocalBuffer(sizeof(int));
            if (!TryReadBytes(s, buffer, sizeof(int)))
            {
                return null;
            }

            return ReadString(s, BinaryPrimitives.ReadInt32BigEndian(buffer));
        }

        /// <summary>
        /// Reads a string with a given length from a stream
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <param name="length">The length to be read</param>
        /// <returns>The string read from stream</returns>
        public static string ReadString(Stream s, int length)
        {
            if (length == (int)SpecialLengths.NULL)
            {
                return null;
            }

            byte[] buffer = GetThreadLocalBuffer(length);
            TryReadBytes(s, buffer, length);
            return Encoding.UTF8.GetString(buffer, 0, length);
        }

        /// <summary>
        /// Reads a byte array with a given length from a stream
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <param name="length">The length to be read</param>
        /// <returns>The a byte array read from stream</returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// An ArgumentOutOfRangeException thrown if the given length is negative
        /// </exception>
        /// <exception cref="ArgumentException">
        /// An ArgumentException if the actual read length is less than the given length
        /// </exception>
        public static byte[] ReadBytes(Stream s, int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length), length, "length can't be negative.");
            }

            var buffer = new byte[length];
            if (length > 0)
            {
                int bytesRead;
                int totalBytesRead = 0;
                do
                {
                    bytesRead = s.Read(buffer, totalBytesRead, length - totalBytesRead);
                    totalBytesRead += bytesRead;
                }
                while ((totalBytesRead < length) && (bytesRead > 0));

                // The stream is closed, return null to notify function caller.
                if (totalBytesRead == 0)
                {
                    return null;
                }

                if (totalBytesRead < length)
                {
                    throw new ArgumentException(
                        $"Incomplete bytes read: {totalBytesRead}, expected: {length}");
                }
            }

            return buffer;
        }

        public static bool TryReadBytes(Stream s, byte[] buffer, int length)
        {
            if (length > 0)
            {
                int bytesRead;
                int totalBytesRead = 0;
                do
                {
                    bytesRead = s.Read(buffer, totalBytesRead, length - totalBytesRead);
                    totalBytesRead += bytesRead;
                }
                while ((totalBytesRead < length) && (bytesRead > 0));

                // The stream is closed, return false to notify function caller.
                if (totalBytesRead == 0)
                {
                    return false;
                }

                if (totalBytesRead < length)
                {
                    throw new ArgumentException(
                        $"Incomplete bytes read: {totalBytesRead}, expected: {length}");
                }
            }

            return true;
        }

        public static int? ReadBytesLength(Stream s)
        {
            byte[] lengthBuffer = ReadBytes(s, sizeof(int));
            if (lengthBuffer == null)
            {
                return null;
            }

            int length = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
            if (length == (int)SpecialLengths.NULL)
            {
                return null;
            }

            return length;
        }

        /// <summary>
        /// Reads a byte array from a stream. The first 4 bytes indicate the length of a byte array.
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The byte array read from stream</returns>
        public static byte[] ReadBytes(Stream s)
        {
            int? length = ReadBytesLength(s);
            if (length == null)
            {
                return null;
            }

            return ReadBytes(s, length.GetValueOrDefault());
        }

        private static byte[] GetThreadLocalBuffer(int minSize)
        {
            const int DefaultMinSize = 256;

            byte[] buffer = s_threadLocalBuffer;
            if (buffer == null || buffer.Length < minSize)
            {
                s_threadLocalBuffer = buffer = new byte[Math.Max(DefaultMinSize, minSize)];
            }

            return buffer;
        }

        /// <summary>
        /// Writes a byte to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The byte to write</param>
        public static void Write(Stream s, byte value) =>
            s.WriteByte(value);

        /// <summary>
        /// Writes a byte array to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The byte array to write</param>
        public static void Write(Stream s, byte[] value) =>
            Write(s, value, value.Length);

        /// <summary>
        /// Writes a byte array to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The byte array to write</param>
        /// <param name="count">The number of bytes in the array to write.</param>
        public static void Write(Stream s, byte[] value, int count) =>
            s.Write(value, 0, count);

        /// <summary>
        /// Writes a boolean to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The boolean value to write</param>
        public static void Write(Stream s, bool value) =>
            Write(s, Convert.ToByte(value));

        /// <summary>
        /// Writes an integer to a stream (big-endian).
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The integer to write</param>
        public static void Write(Stream s, int value)
        {
            byte[] buffer = GetThreadLocalBuffer(sizeof(int));
            BinaryPrimitives.WriteInt32BigEndian(buffer, value);
            Write(s, buffer, sizeof(int));
        }

        /// <summary>
        /// Writes a long integer to a stream (big-endian).
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The long integer to write</param>
        public static void Write(Stream s, long value)
        {
            byte[] buffer = GetThreadLocalBuffer(sizeof(long));
            BinaryPrimitives.WriteInt64BigEndian(buffer, value);
            Write(s, buffer, sizeof(long));
        }

        /// <summary>
        /// Writes a double to a stream (big-endian).
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The double to write</param>
        public static void Write(Stream s, double value) =>
            Write(s, BitConverter.DoubleToInt64Bits(value));

        /// <summary>
        /// Writes a string to a stream.
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The string to write</param>
        public static void Write(Stream s, string value)
        {
            byte[] buffer = GetThreadLocalBuffer(
                sizeof(int) + Encoding.UTF8.GetMaxByteCount(value.Length));
            int len = Encoding.UTF8.GetBytes(value, 0, value.Length, buffer, sizeof(int));
            BinaryPrimitives.WriteInt32BigEndian(buffer, len);
            Write(s, buffer, sizeof(int) + len);
        }
    }
}
