// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Spark.E2ETest;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.UnitTest.TestUtils;
using Moq;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    [CollectionDefinition("E2E hang diagnostic tests", DisableParallelization = true)]
    public sealed class E2EHangDiagnosticsCollection
    {
    }

    [Collection("E2E hang diagnostic tests")]
    public sealed class E2EHangDiagnosticsTests : IDisposable
    {
        public E2EHangDiagnosticsTests() => E2EHangDiagnostics.EndTest();

        [Theory]
        [InlineData(true, 4, "1", true)]
        [InlineData(false, 4, "1", false)]
        [InlineData(true, 3, "1", false)]
        [InlineData(true, 5, "1", false)]
        [InlineData(true, 4, null, false)]
        [InlineData(true, 4, "", false)]
        [InlineData(true, 4, "0", false)]
        [InlineData(true, 4, "true", false)]
        public void ArtifactIsolationBypassRequiresWindowsSpark4AndExplicitFlag(
            bool isWindows, int sparkMajorVersion, string flag, bool expected)
        {
            Assert.Equal(expected, E2EHangDiagnostics.ShouldDisableArtifactIsolation(
                isWindows, sparkMajorVersion, flag));
        }

        [Theory]
        [InlineData("at java.lang.Object.wait(Native Method)")]
        [InlineData("at org.apache.spark.sql.Dataset.collectToPython(Dataset.scala:4164)")]
        [InlineData("at sun.nio.ch.NioSocketImpl.park(java.base@17.0.15/NioSocketImpl.java:186)")]
        [InlineData("at java.lang.Object.wait(java.base@17.0.15/Native Method)")]
        [InlineData("at example.Reader.read(Unknown Source)")]
        [InlineData("java.lang.Thread.State: WAITING (parking)")]
        [InlineData("- parking to wait for  <0x00000001234a> (a java.util.concurrent.FutureTask)")]
        [InlineData("- locked <0x00000001234a> (a java.lang.Object)")]
        [InlineData("- <0x00000001234a> (a java.util.concurrent.locks.ReentrantLock$NonfairSync)")]
        [InlineData("Locked ownable synchronizers:")]
        [InlineData("- None")]
        [InlineData("Found one Java-level deadlock:")]
        public void SanitizePreservesStructuralEvidence(string line)
        {
            Assert.Equal(line, E2EHangDiagnostics.Sanitize("  " + line + "  "));
        }

        [Theory]
        [InlineData("Caused by: java.io.IOException: https://example.invalid/?token=private-value",
            "exception=java.io.IOException message-omitted")]
        [InlineData("Suppressed: java.lang.OutOfMemoryError: private-message",
            "exception=java.lang.OutOfMemoryError message-omitted")]
        [InlineData("26/09/20 01:02:03 ERROR SparkContext: token=private-value",
            "ERROR SparkContext message-omitted")]
        [InlineData("WARN Executor: https://example.invalid/private-message",
            "WARN Executor message-omitted")]
        [InlineData("ERROR TransportResponseHandler: Error installing stream handler. private-value",
            "ERROR TransportResponseHandler event=interceptor-install-failed")]
        [InlineData("ERROR TransportResponseHandler: Could not find callback for StreamResponse.",
            "ERROR TransportResponseHandler event=response-callback-missing")]
        [InlineData("WARN TransportResponseHandler: Stream failure with unknown callback: private-value",
            "WARN TransportResponseHandler event=failure-callback-missing")]
        [InlineData("\"private-thread-name\" #20 daemon prio=5 os_prio=0 cpu=12.50ms elapsed=2.5s " +
            "tid=0x000abc nid=0x123 waiting on condition token=private-value",
            "thread java_thread=20 prio=5 os_prio=0 cpu=12.50ms elapsed=2.5s tid=0x000abc nid=0x123")]
        [InlineData("\"private-thread\" #42 daemon token=private-value",
            "thread java_thread=42")]
        public void SanitizeOmitsMessagesThreadNamesAndArguments(string input, string expected)
        {
            Assert.Equal(expected, E2EHangDiagnostics.Sanitize(input));
        }

        [Theory]
        [InlineData("https://example.invalid/?token=private-value")]
        [InlineData("Authorization: Bearer private-value")]
        [InlineData("JAVA_TOOL_OPTIONS=-Dpassword=private-value")]
        [InlineData("\"private-thread-name\" token=private-value")]
        [InlineData("at example.Reader.read(https://example.invalid/private-value:1)")]
        [InlineData("at example.Reader.read(Reader.java:1) token=private-value")]
        [InlineData("java.lang.Thread.State: WAITING token=private-value")]
        [InlineData("- locked <0x123> (a java.lang.Object) private-value")]
        [InlineData("SELECT * FROM private_data")]
        public void SanitizeRejectsUnrecognizedContent(string line)
        {
            Assert.Null(E2EHangDiagnostics.Sanitize(line));
        }

        [Theory]
        [InlineData("SPARK_TRANSPORT_DIAG event=start seq=1")]
        [InlineData("SPARK_TRANSPORT_DIAG event=response_after channel=1 request=2 " +
            "missing=1 matched=1 callback=3 source_open=1 sink_open=0 source_error=1")]
        [InlineData("SPARK_TRANSPORT_DIAG event=snapshot channel=1 loop=2 thread=3 " +
            "on_loop=0 queued=1 active=0 interceptor=0 bytes_read=-1 timeout_ms=120000 " +
            "loop_shutdown=0 loop_terminated=0 last_request_age_ms=60001")]
        [InlineData("SPARK_TRANSPORT_DIAG event=diagnostic_error error_type=NoSuchFieldException")]
        [InlineData("SPARK_TRANSPORT_DIAG event=snapshot channel=1 request=2 callback=3 " +
            "pipe_source=4 pipe_socket=5 reader_thread=42 reader_matches=1 " +
            "reader_scan_complete=1 blocker_stable=1 reader_supported=1 reader_in_read=1")]
        public void SanitizePreservesAllowlistedTransportMetadata(string line)
        {
            Assert.Equal(line, E2EHangDiagnostics.Sanitize(line));
        }

        [Theory]
        [InlineData("SPARK_TRANSPORT_DIAG event=request uri=spark://private-host/private-class")]
        [InlineData("SPARK_TRANSPORT_DIAG event=request token=123")]
        [InlineData("SPARK_TRANSPORT_DIAG event=private-value channel=1")]
        [InlineData("SPARK_TRANSPORT_DIAG event=request request=private-value")]
        [InlineData("SPARK_TRANSPORT_DIAG event=request request=1\nAuthorization: Bearer private-value")]
        [InlineData("SPARK_TRANSPORT_DIAG event=request request=-2")]
        [InlineData("SPARK_TRANSPORT_DIAG event=diagnostic_error error_type=private-value")]
        [InlineData("SPARK_TRANSPORT_DIAG event=diagnostic_error error_type=IOException message=private-value")]
        [InlineData("SPARK_TRANSPORT_DIAG event=snapshot reader_thread=private-thread")]
        [InlineData("SPARK_TRANSPORT_DIAG event=snapshot pipe_socket=spark://private-host/private-class")]
        public void SanitizeRejectsUnrecognizedTransportFields(string line)
        {
            Assert.Null(E2EHangDiagnostics.Sanitize(line));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TransportStartupIsOptInAndFailuresAreContained(bool enabled)
        {
            using var directory = new TemporaryDirectory();
            var jvm = new Mock<IJvmBridge>(MockBehavior.Strict);
            jvm.Setup(bridge => bridge.CallStaticJavaMethod(
                "org.apache.spark.util.Utils", "getProcessName", It.IsAny<object[]>()))
                .Throws(new InvalidOperationException("identity-unavailable"));
            jvm.Setup(bridge => bridge.CallStaticJavaMethod(
                "org.apache.spark.sql.test.E2ETransportDiagnostics", "start", It.IsAny<object[]>()))
                .Throws(new InvalidOperationException("diagnostics-unavailable"));
            using var diagnostics = new E2EHangDiagnostics(directory.Path, () => { });

            diagnostics.ObserveJvm(jvm.Object, enabled);

            jvm.Verify(bridge => bridge.CallStaticJavaMethod(
                "org.apache.spark.sql.test.E2ETransportDiagnostics", "start", It.IsAny<object[]>()),
                enabled ? Times.Once() : Times.Never());
        }

        [Fact]
        public void TransportEventsDoNotConsumeThreadStackFileBudget()
        {
            using var directory = new TemporaryDirectory();
            using (var diagnostics = new E2EHangDiagnostics(directory.Path, () => { }))
            {
                diagnostics.RecordOutput("jvm-stderr", "SPARK_TRANSPORT_DIAG event=start seq=1");
                diagnostics.RecordOutput("jvm-stack", "java.lang.Thread.State: WAITING");
                Assert.True(SpinWait.SpinUntil(() => Directory.GetFiles(directory.Path).Length == 2,
                    TimeSpan.FromSeconds(5)));
            }

            string[] traces = Directory.GetFiles(directory.Path).Select(File.ReadAllText).ToArray();
            Assert.Contains(traces, trace => trace.Contains("SPARK_TRANSPORT_DIAG") &&
                !trace.Contains("java.lang.Thread.State"));
            Assert.Contains(traces, trace => trace.Contains("java.lang.Thread.State") &&
                !trace.Contains("SPARK_TRANSPORT_DIAG"));
        }

        [Fact]
        public void SanitizeBoundsInputLength()
        {
            Assert.Equal("line-omitted-too-long", E2EHangDiagnostics.Sanitize(new string('x', 4097)));
            Assert.Null(E2EHangDiagnostics.Sanitize(new string('x', 4096)));
        }

        [Fact]
        public void SnapshotsAreLimitedToTwoAndCaptureFailuresDoNotEscape()
        {
            using var directory = new TemporaryDirectory();
            using var captured = new CountdownEvent(2);
            int calls = 0;
            using (var diagnostics = new E2EHangDiagnostics(directory.Path, () =>
            {
                if (Interlocked.Increment(ref calls) <= 2)
                {
                    captured.Signal();
                }

                throw new InvalidOperationException("private-message");
            }, TimeSpan.Zero, TimeSpan.Zero))
            {
                E2EHangDiagnostics.BeginTest();
                Assert.True(captured.Wait(TimeSpan.FromSeconds(5)));
                Assert.False(SpinWait.SpinUntil(() => Volatile.Read(ref calls) > 2, 350));
                E2EHangDiagnostics.EndTest();
            }

            string[] lines = ReadTrace(directory.Path);
            Assert.Equal(2, calls);
            Assert.Equal(2, lines.Count(line => line.Contains("snapshot-begin")));
            Assert.Equal(2, lines.Count(line => line.Contains("snapshot-failed InvalidOperationException")));
            Assert.Equal(2, lines.Count(line => line.Contains("snapshot-end")));
            Assert.Contains(lines, line => line.Contains("testhost cpu-ms=") &&
                line.Contains("working-set=") && line.Contains("threads="));
            Assert.DoesNotContain(lines, line => line.Contains("private-message"));
        }

        [Fact]
        public void EndTestStopsFurtherSnapshotsAndNewTestStartsNewGeneration()
        {
            using var directory = new TemporaryDirectory();
            using var entered = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            int calls = 0;
            using var diagnostics = new E2EHangDiagnostics(directory.Path, () =>
            {
                Interlocked.Increment(ref calls);
                entered.Set();
                release.Wait(TimeSpan.FromSeconds(10));
            }, TimeSpan.Zero, TimeSpan.Zero);
            try
            {
                E2EHangDiagnostics.BeginTest();
                Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                E2EHangDiagnostics.EndTest();
                release.Set();
                Assert.False(SpinWait.SpinUntil(() => Volatile.Read(ref calls) > 1, 350));

                entered.Reset();
                release.Reset();
                E2EHangDiagnostics.BeginTest();
                Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                Assert.Equal(2, Volatile.Read(ref calls));
            }
            finally
            {
                E2EHangDiagnostics.EndTest();
                release.Set();
            }
        }

        [Fact]
        public void OutputQueueIsBoundedAndDropsAreReported()
        {
            using var directory = new TemporaryDirectory();
            using var entered = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            using (var diagnostics = new E2EHangDiagnostics(directory.Path, () =>
            {
                entered.Set();
                release.Wait(TimeSpan.FromSeconds(10));
            }, TimeSpan.Zero))
            {
                try
                {
                    E2EHangDiagnostics.BeginTest();
                    Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                    for (int i = 0; i < 8192; ++i)
                    {
                        diagnostics.RecordOutput("jvm-stderr", "java.lang.Thread.State: WAITING");
                    }

                    diagnostics.RecordOutput("jvm-stderr", "token=private-value");
                    diagnostics.RecordOutput("jvm-stderr", null);
                }
                finally
                {
                    E2EHangDiagnostics.EndTest();
                    release.Set();
                }

                // Dispose deliberately returns after two seconds even if a slow disk is
                // still flushing. Wait for this large synthetic batch before deleting it.
                Assert.True(SpinWait.SpinUntil(() =>
                {
                    Thread.Sleep(25);
                    return ReadTrace(directory.Path).Count(line =>
                        line.Contains("jvm-stderr java.lang.Thread.State")) == 4096;
                }, TimeSpan.FromSeconds(10)));
            }

            string[] lines = ReadTrace(directory.Path);
            Assert.Equal(4096, lines.Count(line => line.Contains("jvm-stderr java.lang.Thread.State")));
            Assert.Contains(lines, line => line.EndsWith("diagnostic-lines-dropped=4096"));
            Assert.DoesNotContain(lines, line => line.Contains("private-value"));
        }

        [Fact]
        public void OutputFileIsBounded()
        {
            using var directory = new TemporaryDirectory();
            using (var diagnostics = new E2EHangDiagnostics(directory.Path, () => { }))
            {
                string frame = "at " + new string('a', 3900) + ".Read(Reader.java:1)";
                for (int batch = 0; batch < 3; ++batch)
                {
                    for (int i = 0; i < 512; ++i)
                    {
                        diagnostics.RecordOutput("jvm-stack", frame);
                    }

                    if (batch < 2)
                    {
                        long minimumLength = (batch + 1) * 512L * 3900;
                        Assert.True(SpinWait.SpinUntil(() =>
                        {
                            string path = Directory.GetFiles(directory.Path).SingleOrDefault();
                            return path != null && new FileInfo(path).Length >= minimumLength;
                        }, TimeSpan.FromSeconds(10)));
                    }
                }
            }

            string tracePath = Assert.Single(Directory.GetFiles(directory.Path));
            Assert.InRange(new FileInfo(tracePath).Length, 1L, 4 * 1024 * 1024L);
            Assert.Equal("trace-size-limit-reached", File.ReadLines(tracePath).Last());
        }

        [Fact]
        public void UnavailableTracePathDoesNotFailTheTest()
        {
            using var directory = new TemporaryDirectory();
            string unavailable = Path.Combine(directory.Path, "existing-file");
            File.WriteAllText(unavailable, "sentinel");
            using var captured = new ManualResetEventSlim();
            using (var diagnostics = new E2EHangDiagnostics(
                unavailable, () => captured.Set(), TimeSpan.Zero))
            {
                E2EHangDiagnostics.BeginTest();
                diagnostics.RecordOutput("jvm-stderr", "java.lang.Thread.State: WAITING");
                Assert.True(captured.Wait(TimeSpan.FromSeconds(5)));
                E2EHangDiagnostics.EndTest();
            }

            Assert.Equal("sentinel", File.ReadAllText(unavailable));
        }

        [Fact]
        public void DisposeDoesNotWaitIndefinitelyForCapture()
        {
            using var directory = new TemporaryDirectory();
            using var entered = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            var diagnostics = new E2EHangDiagnostics(directory.Path, () =>
            {
                entered.Set();
                release.Wait(TimeSpan.FromSeconds(10));
            }, TimeSpan.Zero);
            try
            {
                E2EHangDiagnostics.BeginTest();
                Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                var elapsed = Stopwatch.StartNew();
                diagnostics.Dispose();
                Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(5));
                diagnostics.RecordOutput("jvm-stderr", "java.lang.Thread.State: TERMINATED");
            }
            finally
            {
                E2EHangDiagnostics.EndTest();
                release.Set();
                diagnostics.Dispose();
            }

            Assert.DoesNotContain(ReadTrace(directory.Path), line => line.Contains("TERMINATED"));
        }

        public void Dispose() => E2EHangDiagnostics.EndTest();

        private static string[] ReadTrace(string directory)
        {
            string path = Assert.Single(Directory.GetFiles(directory));
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        }
    }
}
