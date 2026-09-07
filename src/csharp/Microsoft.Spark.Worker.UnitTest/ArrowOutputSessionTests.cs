// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Worker.Command;
using Moq;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class ArrowOutputSessionTests
    {
        [Theory]
        [InlineData(true, 0)]
        [InlineData(false, 0)]
        [InlineData(true, 3)]
        [InlineData(false, 3)]
        public void PreparedBatchesRoundTripWithOneEos(bool legacy, int rowCount)
        {
            using RecordBatch first = CreateBatch(rowCount);
            using RecordBatch second = CreateBatch(rowCount, 100);
            using var output = new ArrowFaultOutputStream();
            var context = new ArrowOutputContext();
            var batches = new TrackingBatches(new[]
            {
                PreparedArrowBatch.Borrow(first),
                PreparedArrowBatch.Borrow(second)
            });

            new ArrowOutputSession(output, Options(legacy), context).Write(batches);

            Assert.Equal(ArrowOutputPhase.Ended, context.Phase);
            Assert.True(context.CanWriteException);
            Assert.Equal(1, batches.DisposeCount);
            Assert.False(output.IsDisposed);
            Assert.Equal(ReferenceOutput(legacy, first, second), output.ToArray());
            output.Position = 0;
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            foreach (int start in new[] { 0, 100 })
            {
                using RecordBatch result = reader.ReadNextRecordBatch();
                Assert.NotNull(result);
                Assert.Equal(rowCount, result.Length);
                Assert.Equal("value", result.Schema.GetFieldByIndex(0).Name);
                var values = Assert.IsType<Int32Array>(result.Column(0));
                for (int i = 0; i < rowCount; ++i)
                {
                    Assert.Equal(start + i, values.GetValue(i));
                }
            }

            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Equal(output.Length, output.Position);
            Assert.All(batches.Batches, batch => Assert.Null(batch.Batch));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        public void FirstPreparationFailureDoesNotStartArrow(int failureSite)
        {
            var primary = new InvalidOperationException("first preparation failed");
            using var output = new ArrowFaultOutputStream();
            var context = new ArrowOutputContext();
            var batches = new TrackingBatches(System.Array.Empty<PreparedArrowBatch>())
            {
                GetEnumeratorFailure = failureSite == 0 ? primary : null,
                MoveNextFailure = failureSite == 1 ? primary : null,
                CurrentFailure = failureSite == 2 ? primary : null,
                FailMoveNextAt = 0,
                PretendFirstBatch = failureSite == 2
            };

            Exception observed = Record.Exception(() =>
                new ArrowOutputSession(output, Options(false), context).Write(batches));

            Assert.Same(primary, observed);
            Assert.Equal(0, output.Length);
            Assert.Equal(ArrowOutputPhase.NotStarted, context.Phase);
            Assert.True(context.CanWriteException);
            Assert.Equal(failureSite == 0 ? 0 : 1, batches.DisposeCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void LaterPreparationFailureEndsArrowBeforeOuterException(bool legacy)
        {
            using RecordBatch source = CreateBatch(2);
            var primary = new InvalidOperationException("later preparation failed");
            var cleanup = new IOException("enumerator cleanup failed");
            using var output = new ArrowFaultOutputStream();
            var context = new ArrowOutputContext();
            var batches = new TrackingBatches(new[] { PreparedArrowBatch.Borrow(source) })
            {
                MoveNextFailure = primary,
                FailMoveNextAt = 1,
                DisposeFailure = cleanup
            };

            Exception observed = Record.Exception(() =>
                new ArrowOutputSession(output, Options(legacy), context).Write(batches));

            Assert.Same(primary, observed);
            Assert.Equal(ArrowOutputPhase.Ended, context.Phase);
            Assert.True(context.CanWriteException);
            Assert.Equal(ReferenceOutput(legacy, source), output.ToArray());
            SerDe.Write(output, (int)SpecialLengths.PYTHON_EXCEPTION_THROWN);
            SerDe.Write(output, observed.ToString());
            output.Position = 0;
            Assert.Equal((int)SpecialLengths.START_ARROW_STREAM, SerDe.ReadInt32(output));
            using var reader = new ArrowStreamReader(output, leaveOpen: true);
            using RecordBatch actual = reader.ReadNextRecordBatch();
            Assert.Equal(2, actual.Length);
            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Equal((int)SpecialLengths.PYTHON_EXCEPTION_THROWN, SerDe.ReadInt32(output));
            Assert.Contains(primary.Message, SerDe.ReadString(output));
            Assert.Equal(output.Length, output.Position);
            Assert.Equal(1, batches.DisposeCount);
        }

        [Theory]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [InlineData(false, false)]
        [InlineData(false, true)]
        public void EveryPartialArrowWriteStopsAllSubsequentProtocolWrites(
            bool legacy,
            bool ioException)
        {
            using RecordBatch source = CreateBatch(2);
            byte[] expected = ReferenceOutput(legacy, source);
            for (int offset = 0; offset < expected.Length; ++offset)
            {
                Exception primary = ioException
                    ? new IOException($"partial write at {offset}")
                    : new InvalidOperationException($"partial write at {offset}");
                using var output = new ArrowFaultOutputStream(offset, primary);
                var context = new ArrowOutputContext();
                var batches = new TrackingBatches(new[] { PreparedArrowBatch.Borrow(source) });

                Exception observed = Record.Exception(() =>
                    new ArrowOutputSession(output, Options(legacy), context).Write(batches));

                Assert.Same(primary, observed);
                Assert.Equal(ArrowOutputPhase.Faulted, context.Phase);
                Assert.False(context.CanWriteException);
                Assert.Equal(expected.Take(offset), output.ToArray());
                Assert.Equal(0, output.ProtocolCallsAfterFailure);
                Assert.Equal(1, batches.DisposeCount);
                Assert.Null(batches.Batches[0].Batch);
                Assert.False(output.IsDisposed);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void FailedEosAndCleanupDoNotReplacePreparationFailure(bool legacy)
        {
            using RecordBatch source = CreateBatch(2);
            byte[] healthy = ReferenceOutput(legacy, source);
            int eosLength = legacy ? 4 : 8;
            var primary = new InvalidOperationException("second batch preparation");
            using var output = new ArrowFaultOutputStream(
                healthy.Length - eosLength + 1,
                new IOException("partial EOS"));
            var context = new ArrowOutputContext();
            var batches = new TrackingBatches(new[] { PreparedArrowBatch.Borrow(source) })
            {
                MoveNextFailure = primary,
                FailMoveNextAt = 1,
                DisposeFailure = new InvalidOperationException("cleanup")
            };

            Exception observed = Record.Exception(() =>
                new ArrowOutputSession(output, Options(legacy), context).Write(batches));

            Assert.Same(primary, observed);
            Assert.Equal(ArrowOutputPhase.Faulted, context.Phase);
            Assert.False(context.CanWriteException);
            Assert.Equal(0, output.ProtocolCallsAfterFailure);
            Assert.Equal(1, batches.DisposeCount);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void BatchCleanupFailureReleasesOtherRootsAndEndsHealthyArrow(bool failWrite)
        {
            using RecordBatch source = CreateBatch(2);
            IArrowArray actual = source.Column(0);
            var cleanup = new InvalidOperationException("owned array cleanup");
            var root = new Mock<IArrowArray>();
            root.SetupGet(array => array.Data).Returns(actual.Data);
            root.SetupGet(array => array.Length).Returns(actual.Length);
            root.Setup(array => array.Accept(It.IsAny<IArrowArrayVisitor>()))
                .Callback<IArrowArrayVisitor>(actual.Accept);
            root.Setup(array => array.Dispose()).Throws(cleanup);
            var otherRoot = new Mock<IArrowArray>();
            var batch = new RecordBatch(source.Schema, new[] { root.Object }, source.Length);
            var prepared = PreparedArrowBatch.Own(batch);
            prepared.AddOwned(otherRoot.Object);
            prepared.Hold(new object());
            var writeFailure = new IOException("write failed first");
            using var output = failWrite
                ? new ArrowFaultOutputStream(2, writeFailure)
                : new ArrowFaultOutputStream();
            var context = new ArrowOutputContext();

            Exception observed = Record.Exception(() =>
                new ArrowOutputSession(output, Options(false), context).Write(new[] { prepared }));

            Assert.Same(failWrite ? writeFailure : cleanup, observed);
            root.Verify(array => array.Dispose(), Times.Once);
            otherRoot.Verify(array => array.Dispose(), Times.Once);
            Assert.Null(prepared.Batch);
            Assert.Equal(0, prepared.HeldReferenceCount);
            Assert.Equal(1, prepared.ReleaseFailedRootCount);
            Assert.Equal(1, prepared.ReleasedRootCount);
            Assert.Equal(0, output.ProtocolCallsAfterFailure);
            if (!failWrite)
            {
                Assert.Equal(ArrowOutputPhase.Ended, context.Phase);
                Assert.Equal(ReferenceOutput(false, source), output.ToArray());
            }
        }

        [Fact]
        public void CompletedOrFaultedSessionCannotRestartAndNewContextIsIndependent()
        {
            using RecordBatch source = CreateBatch(1);
            using var output = new ArrowFaultOutputStream();
            var context = new ArrowOutputContext();
            var session = new ArrowOutputSession(output, Options(false), context);
            session.Write(new[] { PreparedArrowBatch.Borrow(source) });
            long completedLength = output.Length;
            Assert.Throws<InvalidOperationException>(() =>
                session.Write(new[] { PreparedArrowBatch.Borrow(source) }));
            Assert.Equal(completedLength, output.Length);
            context.BeginSuccessTail();
            Assert.False(context.CanWriteException);
            context.BeginSuccessTail();
            Assert.False(context.CanWriteException);

            var freshContext = new ArrowOutputContext();
            Assert.True(freshContext.CanWriteException);
            Assert.False(freshContext.SuccessTailStarted);
            Assert.Equal(ArrowOutputPhase.NotStarted, freshContext.Phase);
        }

        internal static IpcOptions Options(bool legacy) =>
            new IpcOptions { WriteLegacyIpcFormat = legacy };

        internal static RecordBatch CreateBatch(int rowCount, int start = 0)
        {
            var schema = new Schema.Builder()
                .Field(field => field.Name("value").DataType(Int32Type.Default))
                .Build();
            var builder = new Int32Array.Builder();
            for (int i = 0; i < rowCount; ++i)
            {
                builder.Append(start + i);
            }

            return new RecordBatch(schema, new IArrowArray[] { builder.Build() }, rowCount);
        }

        private static byte[] ReferenceOutput(bool legacy, params RecordBatch[] batches)
        {
            using var expected = new MemoryStream();
            SerDe.Write(expected, (int)SpecialLengths.START_ARROW_STREAM);
            using var writer = new ArrowStreamWriter(
                expected, batches[0].Schema, leaveOpen: true, Options(legacy));
            foreach (RecordBatch batch in batches)
            {
                writer.WriteRecordBatch(batch);
            }

            writer.WriteEnd();
            return expected.ToArray();
        }

        private sealed class TrackingBatches : IEnumerable<PreparedArrowBatch>,
            IEnumerator<PreparedArrowBatch>
        {
            private int _index = -1;

            internal TrackingBatches(PreparedArrowBatch[] batches) => Batches = batches;

            internal PreparedArrowBatch[] Batches { get; }
            internal Exception GetEnumeratorFailure { get; set; }
            internal Exception MoveNextFailure { get; set; }
            internal Exception CurrentFailure { get; set; }
            internal Exception DisposeFailure { get; set; }
            internal int FailMoveNextAt { get; set; } = -1;
            internal bool PretendFirstBatch { get; set; }
            internal int DisposeCount { get; private set; }
            public PreparedArrowBatch Current => CurrentFailure != null
                ? throw CurrentFailure
                : Batches[_index];
            object IEnumerator.Current => Current;

            public IEnumerator<PreparedArrowBatch> GetEnumerator() =>
                GetEnumeratorFailure != null ? throw GetEnumeratorFailure : this;

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public bool MoveNext()
            {
                ++_index;
                if (_index == FailMoveNextAt && MoveNextFailure != null)
                {
                    throw MoveNextFailure;
                }

                return _index < Batches.Length || (PretendFirstBatch && _index == 0);
            }

            public void Reset() => throw new NotSupportedException();

            public void Dispose()
            {
                ++DisposeCount;
                if (DisposeFailure != null)
                {
                    throw DisposeFailure;
                }
            }
        }
    }

    /// <summary>
    /// Retains exactly the byte prefix accepted before a write fails, including partial writes.
    /// Counts every later write or flush even when the caller catches the injected exception.
    /// </summary>
    internal sealed class ArrowFaultOutputStream : Stream
    {
        private readonly MemoryStream _bytes = new MemoryStream();
        private readonly long _failOffset;
        private readonly Exception _writeFailure;

        internal ArrowFaultOutputStream(long failOffset = long.MaxValue, Exception writeFailure = null)
        {
            _failOffset = failOffset;
            _writeFailure = writeFailure;
        }

        internal Exception FlushFailure { get; set; }
        internal int FlushCount { get; private set; }
        internal bool HasFailed { get; private set; }
        internal bool IsDisposed { get; private set; }
        internal int ProtocolCallsAfterFailure { get; private set; }
        internal byte[] ToArray() => _bytes.ToArray();
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => !IsDisposed;
        public override long Length => _bytes.Length;
        public override long Position { get => _bytes.Position; set => _bytes.Position = value; }
        public override int Read(byte[] buffer, int offset, int count) => _bytes.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => _bytes.Seek(offset, origin);
        public override void SetLength(long value) => _bytes.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) =>
            Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            if (HasFailed)
            {
                ++ProtocolCallsAfterFailure;
                throw _writeFailure ?? FlushFailure;
            }

            int accepted = (int)Math.Min(buffer.Length, Math.Max(0, _failOffset - _bytes.Length));
            _bytes.Write(buffer.Slice(0, accepted));
            if (accepted != buffer.Length)
            {
                HasFailed = true;
                throw _writeFailure;
            }
        }

        public override void Flush()
        {
            ++FlushCount;
            if (HasFailed)
            {
                ++ProtocolCallsAfterFailure;
                throw _writeFailure ?? FlushFailure;
            }

            if (FlushFailure != null)
            {
                HasFailed = true;
                throw FlushFailure;
            }
        }

        protected override void Dispose(bool disposing)
        {
            IsDisposed = true;
            base.Dispose(disposing);
        }
    }
}
