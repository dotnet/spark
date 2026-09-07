// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Worker.Command
{
    /// <summary>
    /// Owns array roots independently of the schema and struct views used to write a batch.
    /// </summary>
    internal sealed class PreparedArrowBatch : IDisposable
    {
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(PreparedArrowBatch));

        private readonly HashSet<IArrowArray> _roots =
            new HashSet<IArrowArray>(ArrayReferenceComparer.Instance);
        private readonly List<object> _references = new List<object>();
        private bool _disposed;

        internal RecordBatch Batch { get; private set; }

        internal int OwnedRootCount => _roots.Count;

        internal int HeldReferenceCount => _references.Count;

        internal int ReleasedRootCount { get; private set; }

        internal int ReleaseFailedRootCount { get; private set; }

        internal static PreparedArrowBatch Borrow(RecordBatch batch)
        {
            var prepared = new PreparedArrowBatch();
            prepared.ReplaceBatch(batch);
            return prepared;
        }

        internal static PreparedArrowBatch Own(RecordBatch batch)
        {
            var prepared = Borrow(batch);
            try
            {
                foreach (IArrowArray array in batch.Arrays)
                {
                    prepared.AddOwned(array);
                }

                return prepared;
            }
            catch
            {
                // Registration itself can fail. No roots have been disposed yet; clean
                // the original batch by identity without allocating another root set.
                prepared._disposed = true;
                prepared._roots.Clear();
                prepared.Batch = null;
                for (int i = 0; i < batch.ColumnCount; ++i)
                {
                    IArrowArray root = batch.Column(i);
                    bool duplicate = false;
                    for (int j = 0; j < i; ++j)
                    {
                        duplicate |= ReferenceEquals(root, batch.Column(j));
                    }

                    if (!duplicate)
                    {
                        try
                        {
                            root.Dispose();
                        }
                        catch (Exception error)
                        {
                            LogCleanupError(error);
                        }
                    }
                }

                throw;
            }
        }

        internal void ReplaceBatch(RecordBatch batch)
        {
            ThrowIfDisposed();
            Batch = batch ?? throw new ArgumentNullException(nameof(batch));
        }

        internal void AddOwned(IArrowArray array)
        {
            ThrowIfDisposed();
            _roots.Add(array ?? throw new ArgumentNullException(nameof(array)));
        }

        internal void Hold(object reference)
        {
            ThrowIfDisposed();
            _references.Add(reference);
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            // A failed release has uncertain outcome and must never be retried.
            _disposed = true;
            Exception firstError = null;
            foreach (IArrowArray root in _roots)
            {
                try
                {
                    root.Dispose();
                    ++ReleasedRootCount;
                }
                catch (Exception error)
                {
                    ++ReleaseFailedRootCount;
                    if (firstError == null)
                    {
                        firstError = error;
                    }
                    else
                    {
                        LogCleanupError(error);
                    }
                }
            }

            _roots.Clear();
            _references.Clear();
            Batch = null;
            if (firstError != null)
            {
                ExceptionDispatchInfo.Capture(firstError).Throw();
            }
        }

        internal void DisposeAfterFailure()
        {
            try
            {
                Dispose();
            }
            catch (Exception cleanupError)
            {
                LogCleanupError(cleanupError);
            }
        }

        private static void LogCleanupError(Exception error)
        {
            // Diagnostics cannot replace the exception which caused preparation to fail.
            try
            {
                s_logger.LogError($"Arrow batch cleanup failed: {error}");
            }
            catch (Exception)
            {
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(PreparedArrowBatch));
            }
        }

        private sealed class ArrayReferenceComparer : IEqualityComparer<IArrowArray>
        {
            internal static readonly ArrayReferenceComparer Instance = new ArrayReferenceComparer();

            public bool Equals(IArrowArray left, IArrowArray right) => ReferenceEquals(left, right);

            public int GetHashCode(IArrowArray value) => RuntimeHelpers.GetHashCode(value);
        }
    }
}
