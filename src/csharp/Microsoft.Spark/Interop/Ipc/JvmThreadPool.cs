// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// This class corresponds to the ThreadPool we maintain on the JVM side. This class keeps
    /// track of which .NET threads are still alive, and issues an rmThread command if a thread is
    /// not.
    /// </summary>
    internal class JvmThreadPool : IDisposable
    {
        private readonly IJvmBridge _jvmBridge;
        private readonly ConcurrentDictionary<int, Thread> _activeThreads;
        private readonly Timer _activeThreadMonitor;

        /// <summary>
        /// Construct the JvmThreadPool.
        /// </summary>
        /// <param name="jvmBridge">The JvmBridge used to call JVM methods.</param>
        /// <param name="threadGCInterval">The interval to GC finished threads.</param>
        public JvmThreadPool(IJvmBridge jvmBridge, TimeSpan threadGCInterval)
        {
            _jvmBridge = jvmBridge;
            _activeThreads = new ConcurrentDictionary<int, Thread>();
            _activeThreadMonitor = new Timer(
                (state) => GarbageCollectThreads(), null, threadGCInterval, threadGCInterval);
        }

        /// <summary>
        /// Dispose of the thread monitor and run a final round of thread GC.
        /// </summary>
        public void Dispose()
        {
            _activeThreadMonitor.Dispose();
            GarbageCollectThreads();
        }

        /// <summary>
        /// Try to add a thread to the pool.
        /// </summary>
        /// <param name="thread">The thread to add.</param>
        /// <returns>True if success, false if already added.</returns>
        public bool TryAddThread(Thread thread) =>
            _activeThreads.TryAdd(thread.ManagedThreadId, thread);

        /// <summary>
        /// Try to remove a thread.
        /// </summary>
        /// <param name="managedThreadId">The ID of the thread to remove.</param>
        /// <returns>True if success, false if the thread cannot be found.</returns>
        public bool TryRemoveThread(int managedThreadId)
        {
            if (_activeThreads.TryRemove(managedThreadId, out _))
            {
                _jvmBridge.CallStaticJavaMethod("DotnetHandler", "rmThread", managedThreadId);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Remove any threads that are no longer active.
        /// </summary>
        private void GarbageCollectThreads()
        {
            foreach (KeyValuePair<int, Thread> kvp in _activeThreads)
            {
                if (!kvp.Value.IsAlive)
                {
                    TryRemoveThread(kvp.Key);
                }
            }
        }
    }
}
