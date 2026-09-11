// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest
{
    [Collection("Spark E2E Tests")]
    [Trait("Category", "DataFrameRowRetrieval")]
    public class DataFrameRowRetrievalTests
    {
        private readonly SparkFixture _fixture;

        public DataFrameRowRetrievalTests(SparkFixture fixture)
        {
            _fixture = fixture;
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_0_0, Versions.V4_1_0)]
        public void TailReturnsExpectedRows()
        {
            DataFrame frame = _fixture.Spark.Range(0, 12, 1, 3);

            Assert.Equal(new long[] { 9, 10, 11 }, Values(frame.Tail(3)));
            Assert.Equal(Enumerable.Range(0, 12).Select(value => (long)value),
                Values(frame.Tail(20)));
            Assert.Empty(frame.Tail(0));
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_0_0, Versions.V4_1_0)]
        public void TailHandlesEmptyInput()
        {
            Assert.Empty(_fixture.Spark.Range(0, 0, 1, 3).Tail(3));
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_0_0, Versions.V4_1_0)]
        public void DefaultLocalIteratorReturnsRowsAcrossPartitions()
        {
            AssertMixedRows(CreateFrame().ToLocalIterator());
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_0_0, Versions.V4_1_0)]
        public void LocalIteratorReturnsRowsWithAndWithoutPrefetch()
        {
            foreach (bool prefetch in new[] { false, true })
            {
                AssertMixedRows(CreateFrame().ToLocalIterator(prefetch));
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_0_0, Versions.V4_1_0)]
        public void LocalIteratorsHandleEmptyPartitions()
        {
            DataFrame empty = _fixture.Spark.Range(0, 0, 1, 3);
            DataFrame sparse = _fixture.Spark.Range(0, 2, 1, 5);

            Assert.Empty(empty.ToLocalIterator());
            Assert.Equal(new long[] { 0, 1 }, Values(sparse.ToLocalIterator()));
            foreach (bool prefetch in new[] { false, true })
            {
                Assert.Empty(empty.ToLocalIterator(prefetch));
                Assert.Equal(new long[] { 0, 1 }, Values(sparse.ToLocalIterator(prefetch)));
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_0_0, Versions.V4_1_0)]
        public void LocalIteratorCanStopEarly()
        {
            DataFrame frame = _fixture.Spark.Range(0, 12000, 1, 3);
            foreach (IEnumerable<Row> rows in new[]
            {
                frame.ToLocalIterator(),
                frame.ToLocalIterator(false),
                frame.ToLocalIterator(true)
            })
            {
                JvmObjectReference servingThread;
                using (IEnumerator<Row> iterator = rows.GetEnumerator())
                {
                    Assert.True(iterator.MoveNext());
                    Assert.Equal(0L, iterator.Current.GetAs<long>(0));
                    servingThread = GetIteratorServingThread();
                }

                // Observe the actual JVM thread exit, without relying on finalizers or sleeps.
                servingThread.Invoke("join", 10000L);
                Assert.False((bool)servingThread.Invoke("isAlive"));
            }

            Assert.Equal(new long[] { 0 }, Values(frame.ToLocalIterator(true).Take(1)));
            foreach (Row row in frame.ToLocalIterator(false))
            {
                Assert.Equal(0L, row.GetAs<long>(0));
                break;
            }

            Assert.Equal(12000L, frame.Count());
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_0_0, Versions.V4_1_0)]
        public void LocalIteratorStopsServingSocketBeforeCallerClosesIt()
        {
            foreach (bool prefetch in new[] { false, true })
            {
                DataFrame frame = _fixture.Spark.Range(0, 12000, 1, 3);
                var info = (JvmObjectReference[])frame.Reference.Invoke(
                    "toPythonIterator", prefetch);
                using ISocketWrapper socket = SocketFactory.CreateSocket();
                socket.Connect(IPAddress.Loopback,
                    (int)info[0].Invoke("intValue"),
                    (string)info[1].Invoke("toString"));

                using (IEnumerator<Row> iterator =
                    new RowCollector().Collect(socket, info[2]).GetEnumerator())
                {
                    Assert.True(iterator.MoveNext());
                    Assert.Equal(0L, iterator.Current.GetAs<long>(0));
                }

                // The socket is deliberately still open: a clean JVM result proves that
                // Dispose sent stop, rather than relying on disconnect to abort the handler.
                var wait = (JvmObjectReference)_fixture.Jvm.CallStaticJavaMethod(
                    "scala.concurrent.duration.Duration", "create", "10s");
                info[2].Invoke("getResult", wait);
                Assert.Equal(-1, socket.InputStream.ReadByte());
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_1_0, Versions.V4_1_0)]
        public void LocalIteratorPropagatesJvmFailureAndAllowsNextAction()
        {
            const string message = "WI-04 local iterator JVM failure";
            DataFrame failing = _fixture.Spark.Range(0, 3, 1, 3)
                .SelectExpr($"raise_error('{message}')");

            foreach (bool prefetch in new[] { false, true })
            {
                Exception exception = Assert.ThrowsAny<Exception>(() =>
                    failing.ToLocalIterator(prefetch).ToArray());
                Assert.Contains(message, exception.ToString());
                Assert.Equal(new long[] { 0, 1, 2 },
                    Values(_fixture.Spark.Range(3).ToLocalIterator(prefetch)));
            }
        }

        private DataFrame CreateFrame() => _fixture.Spark.Range(0, 12, 1, 3)
            .SelectExpr("id", "concat('row-', cast(id as string)) as label",
                "case when id % 2 = 0 then cast(null as string) else 'odd' end as optional");

        private static long[] Values(IEnumerable<Row> rows) =>
            rows.Select(row => row.GetAs<long>(0)).ToArray();

        private static void AssertMixedRows(IEnumerable<Row> rows)
        {
            Row[] actual = rows.ToArray();
            Assert.Equal(12, actual.Length);
            for (int i = 0; i < actual.Length; ++i)
            {
                Assert.Equal((long)i, actual[i].GetAs<long>("id"));
                Assert.Equal($"row-{i}", actual[i].GetAs<string>("label"));
                Assert.Equal(i % 2 == 0 ? null : "odd", actual[i].GetAs<string>("optional"));
            }
        }

        private JvmObjectReference GetIteratorServingThread()
        {
            var traces = (JvmObjectReference)_fixture.Jvm.CallStaticJavaMethod(
                "java.lang.Thread", "getAllStackTraces");
            var threads = (JvmObjectReference)traces.Invoke("keySet");
            JvmObjectReference threadList = _fixture.Jvm.CallConstructor(
                "java.util.ArrayList", threads);
            return Assert.Single(((JvmObjectReference[])threadList.Invoke("toArray"))
                .Where(thread => (string)thread.Invoke("getName") == "serve toLocalIterator"));
        }
    }
}
