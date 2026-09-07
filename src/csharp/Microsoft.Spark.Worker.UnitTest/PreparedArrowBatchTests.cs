// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Spark.Worker.Command;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class PreparedArrowBatchTests
    {
        [Fact]
        public void DuplicateRootsAndSchemaViewsDoNotCreateAdditionalOwners()
        {
            var root = new TrackingStringArray();
            RecordBatch original = Batch(root, root);
            PreparedArrowBatch prepared = PreparedArrowBatch.Own(original);
            var structType = new StructType(original.Schema.FieldsList);
            var wrapper = new StructArray(structType, 1, original.Arrays, ArrowBuffer.Empty);
            prepared.ReplaceBatch(new RecordBatch(new Schema(new[] { new Field("group", structType, false) }, null),
                new[] { wrapper }, 1));
            prepared.Hold(new byte[1]);
            Assert.Equal(1, prepared.OwnedRootCount);
            prepared.Dispose();
            prepared.Dispose();
            Assert.Equal(1, root.DisposeCalls);
            Assert.Equal(1, prepared.ReleasedRootCount);
            Assert.Equal(0, prepared.ReleaseFailedRootCount);
            Assert.Equal(0, prepared.HeldReferenceCount);
            Assert.Null(prepared.Batch);
        }

        [Fact]
        public void BorrowedCallerArraysRemainOwnedByCaller()
        {
            var root = new TrackingStringArray();
            RecordBatch batch = Batch(root);
            PreparedArrowBatch prepared = PreparedArrowBatch.Borrow(batch);
            prepared.Dispose();
            Assert.Equal(0, root.DisposeCalls);
            Assert.Equal("x", root.GetString(0));
            root.Dispose();
            Assert.Equal(1, root.DisposeCalls);
        }

        [Fact]
        public void CleanupAttemptsEveryDistinctRootAndNeverRetriesUncertainRelease()
        {
            var primary = new InvalidOperationException("first-cleanup-failure");
            var first = new TrackingStringArray(primary);
            var second = new TrackingStringArray();
            var third = new TrackingStringArray(new InvalidOperationException("second-cleanup-failure"));
            PreparedArrowBatch prepared = PreparedArrowBatch.Own(Batch(first, second, third, first));
            prepared.Hold(new object());
            Assert.Same(primary, Assert.Throws<InvalidOperationException>(() => prepared.Dispose()));
            Assert.Equal(1, first.DisposeCalls);
            Assert.Equal(1, second.DisposeCalls);
            Assert.Equal(1, third.DisposeCalls);
            Assert.Equal(1, prepared.ReleasedRootCount);
            Assert.Equal(2, prepared.ReleaseFailedRootCount);
            Assert.Equal(0, prepared.OwnedRootCount);
            Assert.Equal(0, prepared.HeldReferenceCount);
            Assert.Null(prepared.Batch);
            prepared.Dispose();
            Assert.Equal(1, first.DisposeCalls);
            Assert.Throws<ObjectDisposedException>(() => prepared.ReplaceBatch(Batch(second)));
        }

        [Fact]
        public void CleanupAfterPreparationFailureDoesNotThrowAReplacementError()
        {
            var root = new TrackingStringArray(new InvalidOperationException("cleanup"));
            PreparedArrowBatch prepared = PreparedArrowBatch.Own(Batch(root));
            prepared.DisposeAfterFailure();
            Assert.Equal(1, prepared.ReleaseFailedRootCount);
            Assert.Equal(0, prepared.OwnedRootCount);
            Assert.Null(prepared.Batch);
        }

        private static RecordBatch Batch(params IArrowArray[] arrays)
        {
            var fields = new Field[arrays.Length];
            for (int i = 0; i < arrays.Length; ++i)
            {
                fields[i] = new Field("s" + i, StringType.Default, false);
            }

            return new RecordBatch(new Schema(fields, null), arrays, 1);
        }

        private sealed class TrackingStringArray : StringArray, IArrowArray
        {
            private readonly Exception _error;

            internal TrackingStringArray(Exception error = null)
                : base(1, new ArrowBuffer(new byte[] { 0, 0, 0, 0, 1, 0, 0, 0 }),
                    new ArrowBuffer(new byte[] { (byte)'x' }), ArrowBuffer.Empty, 0, 0)
            {
                _error = error;
            }

            internal int DisposeCalls { get; private set; }

            public new void Dispose()
            {
                ++DisposeCalls;
                base.Dispose();
                if (_error != null)
                {
                    throw _error;
                }
            }
        }
    }
}
