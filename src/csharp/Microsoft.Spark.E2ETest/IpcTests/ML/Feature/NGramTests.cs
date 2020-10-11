using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class NGramTests : FeatureBaseTests<NGram>
    {
        private readonly SparkSession _spark;

        public NGramTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestNGram()
        {
            var expectedUid = "theUid";
            var expectedInputCol = "input_col";
            var expectedOutputCol = "output_col";
            var expectedN = 2;

            DataFrame input = _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as input_col");

            var nGram = new NGram(expectedUid)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetN(expectedN);

            var output = nGram.Transform(input);

            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));
            Assert.Equal(expectedInputCol, nGram.GetInputCol());
            Assert.Equal(expectedOutputCol, nGram.GetOutputCol());
            Assert.Equal(expectedN, nGram.GetN());

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "NGram");
                nGram.Save(savePath);

                var loadedNGram = NGram.Load(savePath);
                Assert.Equal(nGram.Uid(), loadedNGram.Uid());
            }

            Assert.Equal(expectedUid, nGram.Uid());

            TestFeatureBase(nGram, "inputCol", "input_col");
        }
    }
}
