// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

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
        /// Test stop words removers without locale,
        /// because locale is not supported before spark 2.4.0 version.
        /// </summary>
        [Fact]
        public void TestStopWordsRemoverWithoutLocale()
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

            using (TemporaryDirectory tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "StopWordsRemover");
                stopWordsRemover.Save(savePath);

                StopWordsRemover loadedStopWordsRemover = StopWordsRemover.Load(savePath);
                Assert.Equal(stopWordsRemover.Uid(), loadedStopWordsRemover.Uid());
            }

            Assert.IsType<StructType>(stopWordsRemover.TransformSchema(input.Schema()));
            Assert.IsType<DataFrame>(stopWordsRemover.Transform(input));

            TestFeatureBase(stopWordsRemover, "inputCol", "input_col");
        }

        /// <summary>
        /// Test stop words removers with locale, run if spark version is greater than spark 2.4.0
        /// skip this test for rest of the spark versions.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestStopWordsRemoverWithLocale()
        {
            string expectedLocale = "en_GB";
            StopWordsRemover stopWordsRemover = new StopWordsRemover().SetLocale(expectedLocale);
            Assert.Equal(expectedLocale, stopWordsRemover.GetLocale());
        }
    }
}
