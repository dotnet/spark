// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class BucketizerTests
    {
        private readonly SparkSession _spark;

        public BucketizerTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestBucketizer()
        {
            double[] expectedSplits = new[] {Double.MinValue, 0.0, 10.0, 50.0, Double.MaxValue};

            string expectedHandle = "skip";
            string expectedUid = "uid";
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";
            
            Bucketizer bucketizer = new Bucketizer(expectedUid)
                .SetInputCol(expectedInputCol)
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
        }

        [Fact]
        public void TestBucketizer_MultipleColumns()
        {
            double[][] expectedSplitsArray = new[]
            {
                new[] {Double.MinValue, 0.0, 10.0, 50.0, Double.MaxValue},
                new[] {Double.MinValue, 0.0, 10000.0, Double.MaxValue}
            };

            string expectedHandle = "keep";

            List<string> expectedInputCols = new List<string>() {"input_col_a", "input_col_b"};
            List<string> expectedOutputCols = new List<string>() {"output_col_a", "output_col_b"};
            
            Bucketizer bucketizer = new Bucketizer()
                .SetInputCols(expectedInputCols)
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
