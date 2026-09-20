// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Spark.E2ETest;
using Microsoft.Spark.UnitTest.TestUtils;
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
        [InlineData("\"private-thread-name\" #20 daemon prio=5 os_prio=0 cpu=12.50ms elapsed=2.5s " +
            "tid=0x000abc nid=0x123 waiting on condition token=private-value",
            "thread prio=5 os_prio=0 cpu=12.50ms elapsed=2.5s tid=0x000abc nid=0x123")]
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
