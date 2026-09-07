// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest
{
    [Collection("Spark E2E Tests")]
    public class Spark40CompatibilityTests
    {
        private readonly SparkFixture _fixture;

        public Spark40CompatibilityTests(SparkFixture fixture)
        {
            _fixture = fixture;
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
        public void RddNonUdfWorkerFailurePropagates()
        {
            const string expectedMessage = "Spark 4 RDD worker delegate failure.";
            RDD<int> result = _fixture.Spark.SparkContext
                .Parallelize(new[] { 1 }, 1)
                .Map<int>(value => throw new InvalidOperationException(expectedMessage));

            Exception exception = Assert.ThrowsAny<Exception>(() => result.Collect().ToArray());
            Assert.Contains(expectedMessage, exception.ToString());
        }
    }
}
