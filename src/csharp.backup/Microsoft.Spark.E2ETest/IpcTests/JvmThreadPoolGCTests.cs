// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Services;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class JvmThreadPoolGCTests
    {
        private readonly ILoggerService _loggerService;
        private readonly SparkSession _spark;
        private readonly IJvmBridge _jvmBridge;

        public JvmThreadPoolGCTests(SparkFixture fixture)
        {
            _loggerService = LoggerServiceFactory.GetLogger(typeof(JvmThreadPoolGCTests));
            _spark = fixture.Spark;
            _jvmBridge = _spark.Reference.Jvm;
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
                    SparkSession activeSession = SparkSession.GetActiveSession();
                    Assert.NotNull(activeSession);
                    Assert.Equal(appName, activeSession.Conf().Get("spark.app.name", null));
                });

                thread.Start();
                thread.Join();
            }

            for (int i = 0; i < 5; ++i)
            {
                testChildThread(i.ToString());
            }

            Assert.Null(SparkSession.GetActiveSession());
        }

        /// <summary>
        /// Monitor a thread via the JvmThreadPoolGC.
        /// </summary>
        [Fact]
        public void TestTryAddThread()
        {
            int processId = Process.GetCurrentProcess().Id;
            using var threadPool = new JvmThreadPoolGC(
                _loggerService, _jvmBridge, TimeSpan.FromMinutes(30), processId);

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
        public void TestRmThread()
        {
            int processId = Process.GetCurrentProcess().Id;
            // Create a thread and ensure that it is initialized in the JVM ThreadPool.
            var thread = new Thread(() => _spark.Sql("SELECT TRUE"));
            thread.Start();
            thread.Join();

            // First call should return true. Second call should return false.
            Assert.True((bool)_jvmBridge.CallStaticJavaMethod("DotnetHandler", "rmThread", processId, thread.ManagedThreadId));
            Assert.False((bool)_jvmBridge.CallStaticJavaMethod("DotnetHandler", "rmThread", processId, thread.ManagedThreadId));
        }

        /// <summary>
        /// Test that the GC interval configuration defaults to 5 minutes, and can be updated
        /// correctly by setting the environment variable.
        /// </summary>
        [Fact]
        public void TestIntervalConfiguration()
        {
            // Default value is 5 minutes.
            Assert.Null(Environment.GetEnvironmentVariable("DOTNET_JVM_THREAD_GC_INTERVAL"));
            Assert.Equal(
                TimeSpan.FromMinutes(5),
                SparkEnvironment.ConfigurationService.JvmThreadGCInterval);

            // Test a custom value.
            Environment.SetEnvironmentVariable("DOTNET_JVM_THREAD_GC_INTERVAL", "1:30:00");
            Assert.Equal(
                TimeSpan.FromMinutes(90),
                SparkEnvironment.ConfigurationService.JvmThreadGCInterval);
        }
    }
}
