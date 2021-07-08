// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
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
        /// Test signatures for APIs up to Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            string expectedUid = "theUidWithOutLocale";
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";
            bool expectedCaseSensitive = false;
            var expectedStopWords = new string[] { "test1", "test2" };

            DataFrame input = _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as input_col");

            StopWordsRemover stopWordsRemover = new StopWordsRemover(expectedUid)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetCaseSensitive(expectedCaseSensitive)
                .SetStopWords(expectedStopWords);

            Assert.Equal(expectedUid, stopWordsRemover.Uid());
            Assert.Equal(expectedInputCol, stopWordsRemover.GetInputCol());
            Assert.Equal(expectedOutputCol, stopWordsRemover.GetOutputCol());
            Assert.Equal(expectedCaseSensitive, stopWordsRemover.GetCaseSensitive());
            Assert.Equal(expectedStopWords, stopWordsRemover.GetStopWords());
            Assert.NotEmpty(StopWordsRemover.LoadDefaultStopWords("english"));

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "StopWordsRemover");
                stopWordsRemover.Save(savePath);

                StopWordsRemover loadedStopWordsRemover = StopWordsRemover.Load(savePath);
                Assert.Equal(stopWordsRemover.Uid(), loadedStopWordsRemover.Uid());
            }

            Assert.IsType<StructType>(stopWordsRemover.TransformSchema(input.Schema()));
            Assert.IsType<DataFrame>(stopWordsRemover.Transform(input));

            TestFeatureBase(stopWordsRemover, "inputCol", "input_col");

            string expectedLocale = "en_GB";
            stopWordsRemover.SetLocale(expectedLocale);
            Assert.Equal(expectedLocale, stopWordsRemover.GetLocale());
        }
    }
}
