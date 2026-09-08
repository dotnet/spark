// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Runtime.InteropServices;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Xunit;
using Xunit.Abstractions;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest
{
    [Collection("Spark E2E Tests")]
    public class Spark40CompatibilityTests
    {
        private readonly SparkFixture _fixture;
        private readonly ITestOutputHelper _output;

        public Spark40CompatibilityTests(SparkFixture fixture, ITestOutputHelper output)
        {
            _fixture = fixture;
            _output = output;
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        [Trait("Category", "Spark40Compatibility")]
        public void DriverBridgeUsesExpectedSparkRuntime()
        {
            Assert.Equal(SparkSettings.Version.ToString(), _fixture.Spark.Version());
            Assert.Equal(44L, _fixture.Spark.Range(44).Count());

            long[] values = _fixture.Spark.Range(4).Collect()
                .Select(row => row.GetAs<long>(0))
                .ToArray();
            Assert.Equal(new long[] { 0, 1, 2, 3 }, values);
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        [Trait("Category", "Spark40Compatibility")]
        public void ScalarUdfExecutesInDotnetWorker()
        {
            DataFrame range = _fixture.Spark.Range(4);
            Func<Column, Column> increment = Udf<long, long>(value => value + 1);

            long[] values = range.Select(increment(range["id"]))
                .Collect()
                .Select(row => row.GetAs<long>(0))
                .ToArray();

            Assert.Equal(new long[] { 1, 2, 3, 4 }, values);
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        [Trait("Category", "Spark40Compatibility")]
        public void ChainedScalarUdfsExecuteInDotnetWorker()
        {
            DataFrame range = _fixture.Spark.Range(4);
            Func<Column, Column> increment = Udf<long, long>(value => value + 1);
            Func<Column, Column> multiplyByTwo = Udf<long, long>(value => value * 2);

            long[] values = range.Select(multiplyByTwo(increment(range["id"])))
                .Collect()
                .Select(row => row.GetAs<long>(0))
                .ToArray();

            Assert.Equal(new long[] { 2, 4, 6, 8 }, values);
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        [Trait("Category", "Spark40Compatibility")]
        public void RddNonUdfPipelineExecutesAcrossPartitions()
        {
            RDD<int> result = _fixture.Spark.SparkContext
                .Parallelize(Enumerable.Range(0, 12), 3)
                .Map(value => value + 1)
                .Filter(value => (value % 2) == 0)
                .MapPartitions(values => values.Select(value => value * 10));

            Assert.Equal(3, result.GetNumPartitions());
            Assert.Equal(new[] { 20, 40, 60, 80, 100, 120 }, result.Collect());
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        [Trait("Category", "Spark40Compatibility")]
        public void RddNonUdfPipelineHandlesEmptyInput()
        {
            RDD<int> result = _fixture.Spark.SparkContext
                .Parallelize(System.Array.Empty<int>(), 2)
                .Map(value => value + 1)
                .Filter(value => value > 0)
                .MapPartitions(values => values.Select(value => value * 10));

            Assert.Equal(2, result.GetNumPartitions());
            Assert.Empty(result.Collect());
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        [Trait("Category", "Spark40Compatibility")]
        [Trait("Category", "Spark40WorkerLifecycle")]
        public void RddWorkerProcessReuseMatchesSparkConfiguration()
        {
            SparkConf conf = _fixture.Spark.SparkContext.GetConf();
            bool reuseWorker = bool.Parse(conf.Get("spark.python.worker.reuse", "true"));
            bool useDaemon = !RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
                bool.Parse(conf.Get("spark.python.use.daemon", "true"));
            RDD<int> workerProcessIds = _fixture.Spark.SparkContext
                .Parallelize(Enumerable.Range(0, 4), 4)
                .Map(value => Environment.ProcessId);

            int[] firstJobProcessIds = workerProcessIds.Collect().ToArray();
            int[] secondJobProcessIds = workerProcessIds.Collect().ToArray();
            int[] processIds = firstJobProcessIds.Concat(secondJobProcessIds).ToArray();
            _output.WriteLine($"Worker daemon: {useDaemon}; reuse: {reuseWorker}; " +
                $"task process IDs: {string.Join(", ", processIds)}");

            Assert.Equal(8, processIds.Length);
            Assert.All(processIds, processId => Assert.True(processId > 0));
            Assert.DoesNotContain(Environment.ProcessId, processIds);

            // SparkFixture uses one local task slot. Only the daemon factory pools
            // workers; the simple factory closes each worker even when reuse is set.
            if (useDaemon && reuseWorker)
            {
                Assert.Single(processIds.Distinct());
            }
            else
            {
                Assert.Equal(processIds.Length, processIds.Distinct().Count());
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        [Trait("Category", "Spark40Compatibility")]
        [Trait("Category", "Spark40WorkerLifecycle")]
        public void ScalarUdfWorkerFailureAllowsSubsequentTask()
        {
            const string expectedMessage = "Spark 4 scalar UDF worker delegate failure.";
            DataFrame range = _fixture.Spark.Range(1);
            Func<Column, Column> fail = Udf<long, long>(value =>
                throw new InvalidOperationException(expectedMessage));

            Exception exception = Assert.ThrowsAny<Exception>(() =>
                range.Select(fail(range["id"])).Collect().ToArray());
            Assert.Contains(expectedMessage, exception.ToString());

            DataFrame recoveryRange = _fixture.Spark.Range(3);
            Func<Column, Column> increment = Udf<long, long>(value => value + 1);
            long[] recoveredValues = recoveryRange.Select(increment(recoveryRange["id"]))
                .Collect()
                .Select(row => row.GetAs<long>(0))
                .ToArray();

            Assert.Equal(new long[] { 1, 2, 3 }, recoveredValues);
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        [Trait("Category", "Spark40Compatibility")]
        [Trait("Category", "Spark40WorkerLifecycle")]
        public void RddNonUdfWorkerFailurePropagates()
        {
            const string expectedMessage = "Spark 4 RDD worker delegate failure.";
            RDD<int> result = _fixture.Spark.SparkContext
                .Parallelize(new[] { 1 }, 1)
                .Map<int>(value => throw new InvalidOperationException(expectedMessage));

            Exception exception = Assert.ThrowsAny<Exception>(() => result.Collect().ToArray());
            Assert.Contains(expectedMessage, exception.ToString());

            int[] recoveredValues = _fixture.Spark.SparkContext
                .Parallelize(new[] { 1, 2, 3, 4 }, 2)
                .Map(value => value + 1)
                .Collect()
                .ToArray();

            Assert.Equal(new[] { 2, 3, 4, 5 }, recoveredValues);
        }
    }
}
