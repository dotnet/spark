using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// This class corresponds to the ThreadPool we maintain on the JVM side. This class keeps
    /// track of which .NET threads are still alive, and issues an rmThread command if a thread is not.
    /// </summary>
    internal class JvmThreadPool
    {
        private readonly IJvmBridge _jvmBridge;
        private readonly ConcurrentDictionary<int, Thread> _activeThreads;
        private readonly Thread _activeThreadMonitor;

        /// <summary>
        /// Construct the JvmThreadPool.
        /// </summary>
        /// <param name="jvmBridge">The JvmBridge used to call JVM methods.</param>
        /// <param name="threadGcInterval">The interval to GC finished threads.</param>
        public JvmThreadPool(IJvmBridge jvmBridge, TimeSpan threadGcInterval)
        {
            _jvmBridge = jvmBridge;
            _activeThreads = new ConcurrentDictionary<int, Thread>();
            _activeThreadMonitor = new Thread(delegate ()
            {
                using var timer = new Timer((state) =>
                {
                    foreach (Thread thread in _activeThreads.Values)
                    {
                        if (!thread.IsAlive)
                        {
                            TryRemoveThread(thread.ManagedThreadId);
                        }
                    }
                }, null, 0, (int)threadGcInterval.TotalMilliseconds);
            });
            _activeThreadMonitor.Start();
        }

        /// <summary>
        /// Try to add a thread to the pool.
        /// </summary>
        /// <param name="thread">The thread to add.</param>
        /// <returns>True if success, false if already added.</returns>
        public bool TryAddThread(Thread thread)
        {
            return _activeThreads.TryAdd(thread.ManagedThreadId, thread);
        }

        /// <summary>
        /// Try to remove a thread.
        /// </summary>
        /// <param name="managedThreadId">The ID of the thread to remove.</param>
        /// <returns>True if success, false if the thread cannot be found.</returns>
        public bool TryRemoveThread(int managedThreadId)
        {
            if (_activeThreads.TryRemove(managedThreadId, out Thread thread))
            {
                _jvmBridge.CallStaticJavaMethod("DotnetHandler", "rmThread", thread.ManagedThreadId);
                return true;
            }

            return false;
        }
    }
}
