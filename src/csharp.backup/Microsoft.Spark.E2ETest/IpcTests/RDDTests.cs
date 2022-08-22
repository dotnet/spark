// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Linq;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class RDDTests
    {
        private readonly SparkContext _sc;

        public RDDTests()
        {
            _sc = SparkContext.GetOrCreate(new SparkConf());
        }

        [Fact]
        public void TestParallelize()
        {
            {
                RDD<int> rdd = _sc.Parallelize(Enumerable.Range(0, 5));
                Assert.Equal(new[] { 0, 1, 2, 3, 4 }, rdd.Collect());
            }
            {
                var strs = new string[] { "hello", "spark", "for", "dotnet" };
                RDD<string> rdd = _sc.Parallelize(strs);
                Assert.Equal(strs, rdd.Collect());
            }
        }

        [Fact]
        public void TestTextFile()
        {
            RDD<string> rdd = _sc.TextFile($"{TestEnvironment.ResourceDirectory}people.txt");
            var strs = new string[] { "Michael, 29", "Andy, 30", "Justin, 19" };
            Assert.Equal(strs, rdd.Collect());

            // Test a transformation so that SerializedMode is correctly propagated.
            RDD<int> intRdd = rdd.Map(str => 0);
            Assert.Equal(new[] { 0, 0, 0 }, intRdd.Collect());
        }

        [Fact]
        public void TestMap()
        {
            RDD<int> rdd = _sc.Parallelize(Enumerable.Range(0, 5))
                .Map(x => x * 2);

            Assert.Equal(new[] { 0, 2, 4, 6, 8 }, rdd.Collect());
        }

        [Fact]
        public void TestFlatMap()
        {
            RDD<string> rdd = _sc.Parallelize(new[] { "hello spark", "for dotnet" })
                .FlatMap(str => str.Split(new char[] { ' ' }));

            Assert.Equal(new[] { "hello", "spark", "for", "dotnet" }, rdd.Collect());
        }

        [Fact]
        public void TestMapPartitions()
        {
            RDD<string> rdd = _sc.Parallelize(Enumerable.Range(0, 5))
                .MapPartitions(inputs => inputs.Select(input => $"str{input}"));

            Assert.Equal(new[] { "str0", "str1", "str2", "str3", "str4" }, rdd.Collect());
        }

        [Fact]
        public void TestMapPartitionsWithIndex()
        {
            RDD<string> rdd = _sc.Parallelize(Enumerable.Range(0, 3))
                .MapPartitionsWithIndex(
                    (pid, inputs) => inputs.Select(input => $"str_{pid}_{input}"));

            Assert.Equal(new[] { "str_0_0", "str_0_1", "str_0_2" }, rdd.Collect());
        }

        [Fact]
        public void TestPipelinedRDD()
        {
            RDD<string> rdd = _sc.Parallelize(Enumerable.Range(0, 3))
                .Map(i => i + 5)
                .Map(i => i * 2)
                .Map(i => $"str_{i}")
                .FlatMap(str => str.Split(new[] { '_' }));

            Assert.Equal(new[] { "str", "10", "str", "12", "str", "14" }, rdd.Collect());
        }

        [Fact]
        public void TestFilter()
        {
            RDD<int> rdd = _sc.Parallelize(Enumerable.Range(0, 5))
                .Filter(x => (x % 2) == 0);

            Assert.Equal(new[] { 0, 2, 4 }, rdd.Collect());
        }

        [Fact]
        public void TestSample()
        {
            RDD<int> rdd = _sc.Parallelize(Enumerable.Range(0, 10))
                .Sample(true, 0.9, 0);

            int count = rdd.Collect().Count();
            Assert.True(count > 0);
            Assert.True(count <= 10);
        }
    }
}
