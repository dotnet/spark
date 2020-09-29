// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Threading;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class JvmThreadPoolGarbageCollectorTests
    {
        private readonly SparkSession _spark;
        private readonly IJvmBridge _jvmBridge;

        public JvmThreadPoolGarbageCollectorTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            _jvmBridge = ((IJvmObjectReferenceProvider)_spark).Reference.Jvm;
        }

        /// <summary>
        /// Test that the active SparkSession is thread-specific.
        /// </summary>
        [Fact]
        public void TestThreadLocalSessions()
        {
            SparkSession.ClearActiveSession();

            void testChildThread(string appName)
            {
                var thread = new Thread(() =>
                {
                    Assert.Null(SparkSession.GetActiveSession());

                    SparkSession.SetActiveSession(
                        SparkSession.Builder().AppName(appName).GetOrCreate());

                    // Since we are in the child thread, GetActiveSession() should return the child
                    // SparkSession.
                    var activeSession = SparkSession.GetActiveSession();
                    Assert.NotNull(activeSession);
                    Assert.Equal(appName, activeSession.Conf().Get("spark.app.name", null));
                });

                thread.Start();
                thread.Join();
            }

            for (var i = 0; i < 5; i++)
            {
                testChildThread(i.ToString());
            }

            Assert.Null(SparkSession.GetActiveSession());
        }

        /// <summary>
        /// Monitor a thread via the JvmThreadPoolGarbageCollector.
        /// </summary>
        [Fact]
        public void TestMonitorThread()
        {
            var threadPool = new JvmThreadPoolGarbageCollector(_jvmBridge, TimeSpan.FromMinutes(30));

            var thread = new Thread(() => _spark.Sql("SELECT TRUE"));
            thread.Start();

            Assert.True(threadPool.TryAddThread(thread));
            // Subsequent call should return false, because the thread has already been added.
            Assert.False(threadPool.TryAddThread(thread));

            thread.Join();
        }

        /// <summary>
        /// Create a Spark worker thread in the JVM ThreadPool then remove it directly through
        /// the JvmBridge.
        /// </summary>
        [Fact]
        public void TestThreadRm()
        {
            // Create a thread and ensure that it is initialized in the JVM ThreadPool.
            var thread = new Thread(() => _spark.Sql("SELECT TRUE"));
            thread.Start();
            thread.Join();
            _jvmBridge.CallStaticJavaMethod("DotnetHandler", "rmThread", thread.ManagedThreadId);
        }
    }
}
