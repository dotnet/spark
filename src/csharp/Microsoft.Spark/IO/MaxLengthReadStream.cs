// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Spark.IO
{
    /// <summary>
    /// Provides a stream wrapper that allows reading only up to the specified number of bytes.
    /// </summary>
    internal sealed class MaxLengthReadStream : Stream
    {
        private Stream _stream;
        private int _remainingAllowedLength;

        public void Reset(Stream stream, int maxLength)
        {
            _stream = stream;
            _remainingAllowedLength = maxLength;
        }

        public override int ReadByte()
        {
            int result = -1;
            if ((_remainingAllowedLength > 0) && (result = _stream.ReadByte()) != -1)
            {
                --_remainingAllowedLength;
            }
            return result;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (count > _remainingAllowedLength)
            {
                count = _remainingAllowedLength;
            }

            int read = _stream.Read(buffer, offset, count);
            _remainingAllowedLength -= read;
            return read;
        }

        public override async Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken)
        {
            if (count > _remainingAllowedLength)
            {
                count = _remainingAllowedLength;
            }

            int read = await _stream.ReadAsync(buffer, offset, count, cancellationToken)
                .ConfigureAwait(false);

            _remainingAllowedLength -= read;
            return read;
        }

        // TODO: On .NET Core 2.1+ / .NET Standard 2.1+, also override ReadAsync that
        // returns ValueTask<int>.

        public override void Flush() => _stream.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) =>
            _stream.FlushAsync();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }
        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }
}
