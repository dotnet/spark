// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class BucketizerTests : FeatureBaseTests<Bucketizer>
    {
        private readonly SparkSession _spark;

        public BucketizerTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Create a <see cref="DataFrame"/>, create a <see cref="Bucketizer"/> and test the
        /// available methods. Test the FeatureBase methods using <see cref="FeatureBaseTests"/>.
        /// </summary>
        [Fact]
        public void TestBucketizer()
        {
            var expectedSplits =
                new double[] { double.MinValue, 0.0, 10.0, 50.0, double.MaxValue };

            string expectedHandle = "skip";
            string expectedUid = "uid";
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";

            var bucketizer = new Bucketizer(expectedUid);
            bucketizer.SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetHandleInvalid(expectedHandle)
                .SetSplits(expectedSplits);

            Assert.Equal(expectedHandle, bucketizer.GetHandleInvalid());

            Assert.Equal(expectedUid, bucketizer.Uid());

            DataFrame input = _spark.Sql("SELECT ID as input_col from range(100)");

            DataFrame output = bucketizer.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));

            Assert.Equal(expectedInputCol, bucketizer.GetInputCol());
            Assert.Equal(expectedOutputCol, bucketizer.GetOutputCol());
            Assert.Equal(expectedSplits, bucketizer.GetSplits());
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "bucket");
                bucketizer.Save(savePath);
                
                Bucketizer loadedBucketizer = Bucketizer.Load(savePath);
                Assert.Equal(bucketizer.Uid(), loadedBucketizer.Uid());
            }
            
            TestFeatureBase(bucketizer, "handleInvalid", "keep");
        }

        [Fact]
        public void TestBucketizer_MultipleColumns()
        {
            var expectedSplitsArray = new double[][]
            {
                new[] { double.MinValue, 0.0, 10.0, 50.0, double.MaxValue},
                new[] { double.MinValue, 0.0, 10000.0, double.MaxValue}
            };

            string expectedHandle = "keep";

            var expectedInputCols = new List<string>() { "input_col_a", "input_col_b" };
            var expectedOutputCols = new List<string>() { "output_col_a", "output_col_b" };

            var bucketizer = new Bucketizer();
            bucketizer.SetInputCols(expectedInputCols)
                .SetOutputCols(expectedOutputCols)
                .SetHandleInvalid(expectedHandle)
                .SetSplitsArray(expectedSplitsArray);

            Assert.Equal(expectedHandle, bucketizer.GetHandleInvalid());

            DataFrame input =
                _spark.Sql("SELECT ID as input_col_a, ID as input_col_b from range(100)");

            DataFrame output = bucketizer.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == "output_col_a"));
            Assert.Contains(output.Schema().Fields, (f => f.Name == "output_col_b"));

            Assert.Equal(expectedInputCols, bucketizer.GetInputCols());
            Assert.Equal(expectedOutputCols, bucketizer.GetOutputCols());
            Assert.Equal(expectedSplitsArray, bucketizer.GetSplitsArray());
        }
    }
}
