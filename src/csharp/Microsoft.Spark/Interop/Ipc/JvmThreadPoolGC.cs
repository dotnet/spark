// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// In .NET for Apache Spark, we maintain a 1-to-1 mapping between .NET application threads
    /// and corresponding JVM threads. When a .NET thread calls a Spark API, that call is executed
    /// by its corresponding JVM thread. This functionality allows for multithreaded applications
    /// with thread-local variables.
    /// 
    /// This class keeps track of the .NET application thread lifecycle. When a .NET application
    /// thread is no longer alive, this class submits an rmThread command to the JVM backend to 
    /// dispose of its corresponding JVM thread. All methods are thread-safe.
    /// </summary>
    internal class JvmThreadPoolGC : IDisposable
    {
        private readonly ILoggerService _loggerService;
        private readonly IJvmBridge _jvmBridge;
        private readonly TimeSpan _threadGCInterval;
        private readonly int _processId;
        private readonly ConcurrentDictionary<int, Thread> _activeThreads;

        private readonly object _activeThreadGCTimerLock;
        private Timer _activeThreadGCTimer;

        /// <summary>
        /// Construct the JvmThreadPoolGC.
        /// </summary>
        /// <param name="loggerService">Logger service.</param>
        /// <param name="jvmBridge">The JvmBridge used to call JVM methods.</param>
        /// <param name="threadGCInterval">The interval to GC finished threads.</param>
        /// <param name="processId"> The ID of the process.</param>
        public JvmThreadPoolGC(ILoggerService loggerService, IJvmBridge jvmBridge, TimeSpan threadGCInterval, int processId)
        {
            _loggerService = loggerService;
            _jvmBridge = jvmBridge;
            _threadGCInterval = threadGCInterval;
            _processId = processId;
            _activeThreads = new ConcurrentDictionary<int, Thread>();

            _activeThreadGCTimerLock = new object();
            _activeThreadGCTimer = null;
        }

        /// <summary>
        /// Dispose of the GC timer and run a final round of thread GC.
        /// </summary>
        public void Dispose()
        {
            lock (_activeThreadGCTimerLock)
            {
                if (_activeThreadGCTimer != null)
                {
                    _activeThreadGCTimer.Dispose();
                    _activeThreadGCTimer = null;
                }
            }

            GCThreads();
        }

        /// <summary>
        /// Try to start monitoring a thread.
        /// </summary>
        /// <param name="thread">The thread to add.</param>
        /// <returns>True if success, false if already added.</returns>
        public bool TryAddThread(Thread thread)
        {
            bool returnValue = _activeThreads.TryAdd(thread.ManagedThreadId, thread);

            // Initialize the GC timer if necessary.
            if (_activeThreadGCTimer == null)
            {
                lock (_activeThreadGCTimerLock)
                {
                    if (_activeThreadGCTimer == null && _activeThreads.Count > 0)
                    {
                        _activeThreadGCTimer = new Timer(
                            (state) => GCThreads(),
                            null,
                            _threadGCInterval,
                            _threadGCInterval);
                    }
                }
            }

            return returnValue;
        }

        /// <summary>
        /// Try to remove a thread from the pool. If the removal is successful, then the
        /// corresponding JVM thread will also be disposed.
        /// </summary>
        /// <param name="threadId">The ID of the thread to remove.</param>
        /// <returns>True if success, false if the thread cannot be found.</returns>
        private bool TryDisposeJvmThread(int threadId)
        {
            if (_activeThreads.TryRemove(threadId, out _))
            {
                // _activeThreads does not have ownership of the threads on the .NET side. This 
                // class does not need to call Join() on the .NET Thread. However, this class is
                // responsible for sending the rmThread command to the JVM to trigger disposal
                // of the corresponding JVM thread.
                if ((bool)_jvmBridge.CallStaticJavaMethod("DotnetHandler", "rmThread", _processId, threadId))
                {
                    _loggerService.LogDebug($"GC'd JVM thread {threadId}.");
                    return true;
                }
                else
                {
                    _loggerService.LogWarn(
                        $"rmThread returned false for JVM thread {threadId}. " +
                        $"Either thread does not exist or has already been GC'd.");
                }
            }

            return false;
        }

        /// <summary>
        /// Remove any threads that are no longer active.
        /// </summary>
        private void GCThreads()
        {
            foreach (KeyValuePair<int, Thread> kvp in _activeThreads)
            {
                if (!kvp.Value.IsAlive)
                {
                    TryDisposeJvmThread(kvp.Key);
                }
            }

            lock (_activeThreadGCTimerLock)
            {
                // Dispose of the timer if there are no threads to monitor.
                if (_activeThreadGCTimer != null && _activeThreads.IsEmpty)
                {
                    _activeThreadGCTimer.Dispose();
                    _activeThreadGCTimer = null;
                }
            }
        }
    }
}
