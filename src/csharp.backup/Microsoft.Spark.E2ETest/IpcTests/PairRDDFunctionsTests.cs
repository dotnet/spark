// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class PairRDDFunctionsTests
    {
        private readonly SparkContext _sc;

        public PairRDDFunctionsTests()
        {
            _sc = SparkContext.GetOrCreate(new SparkConf());
        }

        [Fact]
        public void TestCollect()
        {
            RDD<Tuple<string, int>> rdd = _sc.Parallelize(new[] {
                new Tuple<string, int>("a", 1),
                new Tuple<string, int>("b", 2) });

            // Validate CollectAsMap().
            {
                var expected = new Dictionary<string, int>
                {
                    ["a"] = 1,
                    ["b"] = 2
                };

                Assert.Equal(expected, rdd.CollectAsMap());
            }
            // Validate Keys().
            {
                Assert.Equal(new[] { "a", "b" }, rdd.Keys().Collect());
            }

            // Validate Values().
            {
                Assert.Equal(new[] { 1, 2 }, rdd.Values().Collect());
            }
        }
    }
}
