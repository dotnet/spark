// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using System.Threading.Tasks;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class IpcDebugTraceTests
    {
        [Fact]
        public void TraceContainsCorrelationFieldsAndIsFlushed()
        {
            using var directory = new TemporaryDirectory();
            var trace = new IpcDebugTrace(directory.Path);
            trace.WriteEvent("test-begin SyntheticTest");

            string path = Assert.Single(Directory.GetFiles(directory.Path));
            string line = Assert.Single(File.ReadAllLines(path));
            Assert.Contains("pid=", line);
            Assert.Contains("tid=", line);
            Assert.Contains("dropped=0 test-begin SyntheticTest", line);
        }

        [Fact]
        public void DisabledAndUnavailableTracePathsDoNotFailOperations()
        {
            new IpcDebugTrace(null).WriteEvent("disabled");
            using var directory = new TemporaryDirectory();
            string file = Path.Combine(directory.Path, "not-a-directory");
            File.WriteAllText(file, "sentinel");
            new IpcDebugTrace(file).WriteEvent("unavailable");
            Assert.Equal("sentinel", File.ReadAllText(file));

            string removed = Path.Combine(directory.Path, "removed");
            var trace = new IpcDebugTrace(removed);
            Directory.Delete(removed);
            trace.WriteEvent("write-failed");
            trace.WriteEvent("disabled-after-failure");
            Assert.False(Directory.Exists(removed));
        }

        [Fact]
        public void ConcurrentTraceIsBoundedAndMarksTruncation()
        {
            using var directory = new TemporaryDirectory();
            var trace = new IpcDebugTrace(directory.Path, maxBytes: 4096);
            Parallel.For(0, 1000, i => trace.WriteEvent($"synthetic-event {i}"));
            // Complete the bound check independently of contention-related drops.
            for (int i = 0; i < 100; ++i)
            {
                trace.WriteEvent("synthetic-event");
            }

            string path = Assert.Single(Directory.GetFiles(directory.Path));
            Assert.InRange(new FileInfo(path).Length, 1L, 4096L);
            string[] lines = File.ReadAllLines(path);
            Assert.Equal("trace-size-limit-reached", lines[lines.Length - 1]);
            for (int i = 0; i < lines.Length - 1; ++i)
            {
                Assert.Contains("synthetic-event", lines[i]);
                Assert.Contains("dropped=", lines[i]);
            }
        }
    }
}
