using System;
using System.Collections.Generic;
using System.Linq;
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
            Bucketizer bucketizer = new Bucketizer("uid")
                .SetInputCol("input_col")
                .SetOutputCol("output_col")
                .SetHandleInvalid(Bucketizer.BucketizerInvalidOptions.skip)
                .SetSplits(new[] {Double.MinValue, 0.0, 10.0, 50.0, Double.MaxValue});

            Assert.Equal(Bucketizer.BucketizerInvalidOptions.skip,
                bucketizer.GetHandleInvalid());

            Assert.Equal("uid", bucketizer.Uid());
            
            DataFrame input = _spark.Sql("SELECT ID as input_col from range(100)");

            DataFrame output = bucketizer.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == "output_col"));
        }
        
        [Fact]
        public void TestBucketizer_MultipleColumns()
        {
            Bucketizer bucketizer = new Bucketizer()
                .SetInputCols(new List<string>(){"input_col_a", "input_col_b"})
                .SetOutputCols(new List<string>(){"output_col_a", "output_col_b"})
                .SetHandleInvalid(Bucketizer.BucketizerInvalidOptions.keep)
                .SetSplitsArray(new []{
                    new[] {Double.MinValue, 0.0, 10.0, 50.0, Double.MaxValue},
                    new[] {Double.MinValue, 0.0, 10000.0, Double.MaxValue}
                });

            Assert.Equal(Bucketizer.BucketizerInvalidOptions.keep,
                bucketizer.GetHandleInvalid());

            DataFrame input = 
                _spark.Sql("SELECT ID as input_col_a, ID as input_col_b from range(100)");

            DataFrame output = bucketizer.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == "output_col_a"));
            Assert.Contains(output.Schema().Fields, (f => f.Name == "output_col_b"));
        }
    }
}
