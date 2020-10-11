using System.IO;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
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
        public void TestStopWordsRemoverWithoutLocale()
        {
            var expectedUid = "theUidWithOutLocale";
            var expectedInputCol = "input_col";
            var expectedOutputCol = "output_col";
            var expectedCaseSensitive = false;
            var expectedStopWords = new string[] {"test1", "test2"};
            var expectedSchema = new StructType(new[]
            {
                new StructField("input_col", new ArrayType(new StringType(), true)),
                new StructField("output_col", new ArrayType(new StringType(), true))
            });

            var input = _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as input_col");

            var stopWordsRemover = new StopWordsRemover(expectedUid)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetCaseSensitive(expectedCaseSensitive)
                .SetStopWords(expectedStopWords);

            var outPutSchema = stopWordsRemover.TransformSchema(input.Schema());

            var output = stopWordsRemover.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));
            Assert.Equal(expectedInputCol, stopWordsRemover.GetInputCol());
            Assert.Equal(expectedOutputCol, stopWordsRemover.GetOutputCol());
            Assert.Equal(expectedCaseSensitive, stopWordsRemover.GetCaseSensitive());
            Assert.Equal(expectedStopWords, stopWordsRemover.GetStopWords());
            Assert.Equal(expectedSchema, outPutSchema);

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

        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestStopWordsRemoverWithLocale()
        {
            var expectedUid = "theUidWithLocale";
            var expectedInputCol = "input_col";
            var expectedOutputCol = "output_col";
            var expectedLocale = "en_GB";
            var expectedCaseSensitive = false;
            var expectedStopWords = new string[] {"test1", "test2"};

            var input = _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as input_col");

            var stopWordsRemover = new StopWordsRemover(expectedUid)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetCaseSensitive(expectedCaseSensitive)
                .SetLocale(expectedLocale)
                .SetStopWords(expectedStopWords);

            var output = stopWordsRemover.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));
            Assert.Equal(expectedInputCol, stopWordsRemover.GetInputCol());
            Assert.Equal(expectedOutputCol, stopWordsRemover.GetOutputCol());
            Assert.Equal(expectedLocale, stopWordsRemover.GetLocale());
            Assert.Equal(expectedCaseSensitive, stopWordsRemover.GetCaseSensitive());
            Assert.Equal(expectedStopWords, stopWordsRemover.GetStopWords());

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
