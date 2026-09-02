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
    }
}
