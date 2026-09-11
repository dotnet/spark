// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Processor;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class EncryptedBroadcastReaderTests
    {
        [Theory]
        [InlineData(8192, 16384)]
        [InlineData(1073741824, 0x7FFFFFC7)]
        [InlineData(0x7FFFFFC6, 0x7FFFFFC7)]
        public void BufferGrowthSaturatesAtRuntimeArrayLimit(int capacity, int expected)
        {
            Assert.Equal(expected, EncryptedBroadcastReader.GetExpandedCapacity(capacity));
        }

        [Fact]
        public void BufferGrowthBeyondRuntimeArrayLimitIsRejected()
        {
            Assert.Throws<InvalidDataException>(() =>
                EncryptedBroadcastReader.GetExpandedCapacity(0x7FFFFFC7));
        }

        [Theory]
        [InlineData(1, 37)]
        [InlineData(7, 20000)]
        [InlineData(8192, 200000)]
        public void FragmentedObjectsPreserveFollowingRawIds(int fragmentSize, int valueLength)
        {
            const long FirstId = 4294967301L;
            const long SecondId = 4294967302L;
            const long Sentinel = 9876543210L;
            string value = new string('x', valueLength);
            using var bytes = new MemoryStream();
            SerDe.Write(bytes, FirstId);
            BinarySerDe.Serialize(bytes, value);
            SerDe.Write(bytes, SecondId);
            BinarySerDe.Serialize(bytes, new[] { 17, 29, 31 });
            SerDe.Write(bytes, Sentinel);
            using var stream = new FragmentedStream(bytes.ToArray(), fragmentSize);
            var reader = new EncryptedBroadcastReader(stream);

            Assert.Equal(FirstId, reader.ReadId());
            Assert.Equal(value, reader.ReadValue());
            Assert.Equal(SecondId, reader.ReadId());
            Assert.Equal(new[] { 17, 29, 31 }, Assert.IsType<int[]>(reader.ReadValue()));
            Assert.Equal(Sentinel, reader.ReadId());
            Assert.Throws<EndOfStreamException>(() => reader.ReadId());
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(7)]
        public void TruncatedRawIdIsRejected(int byteCount)
        {
            using var stream = new FragmentedStream(new byte[byteCount], 1);
            var reader = new EncryptedBroadcastReader(stream);

            Assert.Throws<EndOfStreamException>(() => reader.ReadId());
        }

        [Fact]
        public void TruncatedObjectDoesNotBecomeACompletedValue()
        {
            // A string header declares four bytes but the producer sent only one.
            using var stream = new FragmentedStream(new byte[] { 0xd9, 4, (byte)'x' }, 1);
            var reader = new EncryptedBroadcastReader(stream);

            Assert.Throws<EndOfStreamException>(() => reader.ReadValue());
        }

        [Fact]
        public void MultipleCoalescedPrimitiveValuesDoNotNeedServerEof()
        {
            // Raw ID 1 + MessagePack nil + raw ID 2 + MessagePack true.
            byte[] bytes =
            {
                0, 0, 0, 0, 0, 0, 0, 1, 0xc0,
                0, 0, 0, 0, 0, 0, 0, 2, 0xc3
            };
            using var stream = new FragmentedStream(bytes, bytes.Length, forbidEofRead: true);
            var reader = new EncryptedBroadcastReader(stream);

            Assert.Equal(1, reader.ReadId());
            Assert.Null(reader.ReadValue());
            Assert.Equal(2, reader.ReadId());
            Assert.Equal(true, reader.ReadValue());
        }

        private sealed class FragmentedStream : Stream
        {
            private readonly MemoryStream _stream;
            private readonly int _fragmentSize;
            private readonly bool _forbidEofRead;

            internal FragmentedStream(byte[] bytes, int fragmentSize, bool forbidEofRead = false)
            {
                _stream = new MemoryStream(bytes);
                _fragmentSize = fragmentSize;
                _forbidEofRead = forbidEofRead;
            }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (_forbidEofRead && _stream.Position == _stream.Length)
                {
                    throw new InvalidOperationException("The reader must not wait for server EOF.");
                }

                return _stream.Read(buffer, offset, Math.Min(count, _fragmentSize));
            }

            public override void Flush() => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) =>
                throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) =>
                throw new NotSupportedException();

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _stream.Dispose();
                }

                base.Dispose(disposing);
            }
        }
    }
}
