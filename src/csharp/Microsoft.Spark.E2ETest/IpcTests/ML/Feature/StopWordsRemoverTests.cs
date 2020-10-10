using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class StopWordsRemoverTests : FeatureBaseTests<StopWordsRemover>
    {
        private readonly SparkSession _spark;

        public StopWordsRemoverTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestStopWordsRemover()
        {
            string expectedUid = "theUid";
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";

            var input = _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as input_col");

            var stopWordsRemover = new StopWordsRemover(expectedUid)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol);

            var output = stopWordsRemover.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));
            Assert.Equal(expectedInputCol, stopWordsRemover.GetInputCol());
            Assert.Equal(expectedOutputCol, stopWordsRemover.GetOutputCol());

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "StopWordsRemover");
                stopWordsRemover.Save(savePath);

                var loadedStopWordsRemover = StopWordsRemover.Load(savePath);
                Assert.Equal(stopWordsRemover.Uid(), loadedStopWordsRemover.Uid());
            }

            Assert.Equal(expectedUid, stopWordsRemover.Uid());

            TestFeatureBase(stopWordsRemover, "inputCol", "input_col");
        }
    }
}
