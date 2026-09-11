// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Moq;
using Razorvine.Pickle;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class RowCollectorTests
    {
        [Fact]
        public void CollectEmptyIteratorRequestsCompletionOnce()
        {
            using var input = new MemoryStream(new byte[] { 0, 0, 0, 0 });
            using var output = new TestStream();
            Mock<ISocketWrapper> socket = CreateSocket(input, output);

            Assert.Empty(new RowCollector().Collect(socket.Object, null));

            Assert.Equal(input.Length, input.Position);
            AssertRequests(output, 1);
            Assert.Equal(1, output.FlushCount);
            socket.Verify(m => m.Dispose(), Times.Never);
        }

        [Fact]
        public void CollectReadsMultipleBatchesAndEmptyPartitions()
        {
            using var input = new MemoryStream();
            SerDe.Write(input, 1);
            WriteBatch(input, 10, 11);
            WriteBatch(input, 12);
            SerDe.Write(input, -1);
            SerDe.Write(input, 1);
            SerDe.Write(input, -1);
            SerDe.Write(input, 1);
            WriteBatch(input, 20, 21);
            SerDe.Write(input, -1);
            SerDe.Write(input, 0);
            input.Position = 0;
            using var output = new TestStream();
            Mock<ISocketWrapper> socket = CreateSocket(input, output);

            int[] values = new RowCollector().Collect(socket.Object, null)
                .Select(row => row.GetAs<int>(0)).ToArray();

            Assert.Equal(new[] { 10, 11, 12, 20, 21 }, values);
            Assert.Equal(input.Length, input.Position);
            AssertRequests(output, 1, 1, 1, 1);
            Assert.Equal(4, output.FlushCount);
            socket.Verify(m => m.Dispose(), Times.Never);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public void DisposeDrainsOnlyActivePartitionAndStopsOnce(int consumedRows)
        {
            using var input = new MemoryStream();
            SerDe.Write(input, 1);
            WriteBatch(input, 10, 11);
            WriteBatch(input, 12);
            SerDe.Write(input, -1);
            long partitionEnd = input.Position;
            SerDe.Write(input, 1);
            WriteBatch(input, 20);
            SerDe.Write(input, -1);
            SerDe.Write(input, 0);
            input.Position = 0;
            using var output = new TestStream();
            Mock<ISocketWrapper> socket = CreateSocket(input, output);
            using IEnumerator<Row> iterator =
                new RowCollector().Collect(socket.Object, null).GetEnumerator();

            for (int i = 0; i < consumedRows; ++i)
            {
                Assert.True(iterator.MoveNext());
                Assert.Equal(10 + i, iterator.Current.GetAs<int>(0));
            }

            iterator.Dispose();
            iterator.Dispose();

            Assert.False(iterator.MoveNext());
            Assert.Equal(partitionEnd, input.Position);
            AssertRequests(output, 1, 0);
            Assert.Equal(2, output.FlushCount);
            socket.Verify(m => m.Dispose(), Times.Never);
        }

        [Fact]
        public void DisposeUnstartedIteratorDoesNotTouchSocket()
        {
            var socket = new Mock<ISocketWrapper>(MockBehavior.Strict);
            IEnumerator<Row> iterator =
                new RowCollector().Collect(socket.Object, null).GetEnumerator();

            iterator.Dispose();
            iterator.Dispose();

            socket.VerifyNoOtherCalls();
        }

        [Theory]
        [InlineData("read")]
        [InlineData("write")]
        [InlineData("flush")]
        public void CleanupFailureDoesNotMaskConsumerException(string failedOperation)
        {
            using var input = new TestStream();
            SerDe.Write(input, 1);
            WriteBatch(input, 10);
            WriteBatch(input, 11);
            SerDe.Write(input, -1);
            long partitionEnd = input.Position;
            SerDe.Write(input, 0);
            input.Position = 0;
            using var output = new TestStream();
            Mock<ISocketWrapper> socket = CreateSocket(input, output);
            var expected = new InvalidOperationException("Consumer failed.");
            var cleanupFailure = new IOException("Cleanup failed.");

            InvalidOperationException actual = Assert.Throws<InvalidOperationException>(() =>
            {
                foreach (Row row in new RowCollector().Collect(socket.Object, null))
                {
                    Assert.Equal(10, row.GetAs<int>(0));
                    input.ReadFailure = failedOperation == "read" ? cleanupFailure : null;
                    output.WriteFailure = failedOperation == "write" ? cleanupFailure : null;
                    output.FlushFailure = failedOperation == "flush" ? cleanupFailure : null;
                    throw expected;
                }
            });

            Assert.Same(expected, actual);
            Assert.Equal(1, input.FailureCount + output.FailureCount);
            if (failedOperation == "read")
            {
                Assert.True(input.Position < partitionEnd);
            }
            else
            {
                Assert.Equal(partitionEnd, input.Position);
            }

            AssertRequests(output, failedOperation == "flush" ? new[] { 1, 0 } : new[] { 1 });
            socket.Verify(m => m.Dispose(), Times.Never);
        }

        [Fact]
        public void CleanupFailureDoesNotMaskPartitionReadException()
        {
            using var input = new TestStream();
            SerDe.Write(input, 1);
            WriteBatch(input, 10);
            WriteBatch(input, 11);
            SerDe.Write(input, -1);
            input.Position = 0;
            using var output = new TestStream();
            Mock<ISocketWrapper> socket = CreateSocket(input, output);
            using IEnumerator<Row> iterator =
                new RowCollector().Collect(socket.Object, null).GetEnumerator();
            Assert.True(iterator.MoveNext());
            var expected = new IOException("Partition read failed.");
            input.ReadFailure = expected;
            output.WriteFailure = new IOException("Stop write failed.");

            Assert.Same(expected, Assert.Throws<IOException>(() => iterator.MoveNext()));

            Assert.Equal(1, input.FailureCount);
            Assert.Equal(1, output.FailureCount);
            AssertRequests(output, 1);
            socket.Verify(m => m.Dispose(), Times.Never);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void JvmErrorPropagatesWithoutSendingStop(bool afterPartition)
        {
            using var input = new MemoryStream();
            if (afterPartition)
            {
                SerDe.Write(input, 1);
                WriteBatch(input, 10);
                SerDe.Write(input, -1);
            }

            SerDe.Write(input, -1);
            input.Position = 0;
            using var output = new TestStream();
            Mock<ISocketWrapper> socket = CreateSocket(input, output);
            var bridge = new Mock<IJvmBridge>();
            var server = new JvmObjectReference("iterator-server", bridge.Object);
            var expected = new InvalidOperationException("JVM partition job failed.");
            bridge.Setup(m => m.CallNonStaticJavaMethod(
                server, "getResult", It.IsAny<object[]>())).Throws(expected);

            Assert.Same(expected, Assert.Throws<InvalidOperationException>(() =>
                new RowCollector().Collect(socket.Object, server).ToArray()));

            Assert.Equal(input.Length, input.Position);
            AssertRequests(output, afterPartition ? new[] { 1, 1 } : new[] { 1 });
            bridge.Verify(m => m.CallNonStaticJavaMethod(
                server, "getResult", It.Is<object[]>(args => args.Length == 0)), Times.Once);
            socket.Verify(m => m.Dispose(), Times.Never);
        }

        private static Mock<ISocketWrapper> CreateSocket(Stream input, Stream output)
        {
            var socket = new Mock<ISocketWrapper>();
            socket.Setup(m => m.InputStream).Returns(input);
            socket.Setup(m => m.OutputStream).Returns(output);
            return socket;
        }

        private static void WriteBatch(Stream stream, params int[] values)
        {
            new StructTypePickler().Register();
            new TestUtils.RowPickler().Register();
            var schema = new StructType(new[] { new StructField("value", new IntegerType()) });
            Row[] rows = values.Select(value => new Row(new object[] { value }, schema)).ToArray();
            byte[] batch = new Pickler().dumps(rows);
            SerDe.Write(stream, batch.Length);
            SerDe.Write(stream, batch);
        }

        private static void AssertRequests(MemoryStream output, params int[] requests)
        {
            // Check the actual big-endian protocol bytes without using the production reader.
            byte[] expected = requests.SelectMany(request =>
                new byte[] { 0, 0, 0, (byte)request }).ToArray();
            Assert.Equal(expected, output.ToArray());
        }

        private sealed class TestStream : MemoryStream
        {
            internal IOException ReadFailure { get; set; }

            internal IOException WriteFailure { get; set; }

            internal IOException FlushFailure { get; set; }

            internal int FailureCount { get; private set; }

            internal int FlushCount { get; private set; }

            public override int Read(byte[] buffer, int offset, int count)
            {
                ThrowIfFailed(ReadFailure);
                return base.Read(buffer, offset, count);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                ThrowIfFailed(WriteFailure);
                base.Write(buffer, offset, count);
            }

            public override void Flush()
            {
                ++FlushCount;
                ThrowIfFailed(FlushFailure);
                base.Flush();
            }

            private void ThrowIfFailed(IOException failure)
            {
                if (failure != null)
                {
                    ++FailureCount;
                    throw failure;
                }
            }
        }
    }
}
