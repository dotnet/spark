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


        /// <summary>
        /// Test stop words removers without locale, because locale is not supported before spark 2.4.0 version.
        /// </summary>
        [Fact]
        public void TestStopWordsRemoverWithoutLocale()
        {
            string expectedUid = "theUidWithOutLocale";
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";
            bool expectedCaseSensitive = false;
            string[] expectedStopWords = new string[] {"test1", "test2"};
            StructType expectedSchema = new StructType(new[]
            {
                new StructField("input_col", new ArrayType(new StringType(), true)),
                new StructField("output_col", new ArrayType(new StringType(), true))
            });

            DataFrame input = _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as input_col");

            StopWordsRemover stopWordsRemover = new StopWordsRemover(expectedUid)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetCaseSensitive(expectedCaseSensitive)
                .SetStopWords(expectedStopWords);

            StructType outPutSchema = stopWordsRemover.TransformSchema(input.Schema());

            DataFrame output = stopWordsRemover.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));
            Assert.Equal(expectedInputCol, stopWordsRemover.GetInputCol());
            Assert.Equal(expectedOutputCol, stopWordsRemover.GetOutputCol());
            Assert.Equal(expectedCaseSensitive, stopWordsRemover.GetCaseSensitive());
            Assert.Equal(expectedStopWords, stopWordsRemover.GetStopWords());
            Assert.Equal(expectedSchema, outPutSchema);

            using (TemporaryDirectory tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "StopWordsRemover");
                stopWordsRemover.Save(savePath);

                StopWordsRemover loadedStopWordsRemover = StopWordsRemover.Load(savePath);
                Assert.Equal(stopWordsRemover.Uid(), loadedStopWordsRemover.Uid());
            }

            Assert.Equal(expectedUid, stopWordsRemover.Uid());

            TestFeatureBase(stopWordsRemover, "inputCol", "input_col");
        }

        /// <summary>
        /// Test stop words removers with locale, run if spark version is greater than spark 2.4.0
        /// skip this test for rest of the spark versions.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestStopWordsRemoverWithLocale()
        {
            string expectedUid = "theUidWithLocale";
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";
            string expectedLocale = "en_GB";
            bool expectedCaseSensitive = false;
            string[] expectedStopWords = new string[] {"test1", "test2"};

            DataFrame input = _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as input_col");

            StopWordsRemover stopWordsRemover = new StopWordsRemover(expectedUid)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetCaseSensitive(expectedCaseSensitive)
                .SetLocale(expectedLocale)
                .SetStopWords(expectedStopWords);

            DataFrame output = stopWordsRemover.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));
            Assert.Equal(expectedInputCol, stopWordsRemover.GetInputCol());
            Assert.Equal(expectedOutputCol, stopWordsRemover.GetOutputCol());
            Assert.Equal(expectedLocale, stopWordsRemover.GetLocale());
            Assert.Equal(expectedCaseSensitive, stopWordsRemover.GetCaseSensitive());
            Assert.Equal(expectedStopWords, stopWordsRemover.GetStopWords());

            using (TemporaryDirectory tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "StopWordsRemover");
                stopWordsRemover.Save(savePath);

                StopWordsRemover loadedStopWordsRemover = StopWordsRemover.Load(savePath);
                Assert.Equal(stopWordsRemover.Uid(), loadedStopWordsRemover.Uid());
            }

            Assert.Equal(expectedUid, stopWordsRemover.Uid());

            TestFeatureBase(stopWordsRemover, "inputCol", "input_col");
        }
    }
}
