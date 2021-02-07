using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class BinarizerTests : FeatureBaseTests<Binarizer>
    {
        private readonly SparkSession _spark;

        public BinarizerTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestBinarizer()
        {
            string inputCol = "feature";
            DataFrame input = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {0, 0.1}),
                    new GenericRow(new object[] {1, 0.8}),
                    new GenericRow(new object[] {2, 0.2})
                },
                new StructType(new List<StructField>
                {
                    new StructField("id", new IntegerType()), new StructField(inputCol, new DoubleType())
                }));
            string expectedUid = "theUid";
            string outputCol = "binarized_feature";
            double threshold = 0.5;
            Binarizer binarizer = new Binarizer(expectedUid)
                .SetInputCol(inputCol)
                .SetOutputCol(outputCol)
                .SetThreshold(threshold);
            DataFrame output = binarizer.Transform(input);
            StructType outputSchema = binarizer.TransformSchema(input.Schema());

            Assert.Contains(output.Schema().Fields, (f => f.Name == outputCol));
            Assert.Contains(outputSchema.Fields, (f => f.Name == outputCol));
            Assert.Equal(inputCol, binarizer.GetInputCol());
            Assert.Equal(outputCol, binarizer.GetOutputCol());
            Assert.Equal(threshold, binarizer.GetThreshold());

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "Binarizer");
                binarizer.Save(savePath);

                Binarizer loadedBinarizer = Binarizer.Load(savePath);
                Assert.Equal(loadedBinarizer.Uid(), binarizer.Uid());
            }

            Assert.Equal(expectedUid, binarizer.Uid());
        }

        [Fact]
        public void TestBinarizerWithArrayParams()
        {
            string[] inputCol = new[] {"col1", "col2"};
            string[] outputCol = new[] {"feature1", "feature2"};
            double[] threshold = new[] {0.5, 0.8};
            Binarizer binarizer = new Binarizer()
                .SetInputCols(inputCol)
                .SetOutputCols(outputCol)
                .SetThresholds(threshold);

            Assert.Equal(inputCol, binarizer.GetInputCols());
            Assert.Equal(outputCol, binarizer.GetOutputCols());
            Assert.Equal(threshold, binarizer.GetThresholds());
        }
    }
}
