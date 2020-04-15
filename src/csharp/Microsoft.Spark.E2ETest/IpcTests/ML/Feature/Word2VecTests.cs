// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class Word2VecTests
    {
        private readonly SparkSession _spark;

        public Word2VecTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestWord2Vec()
        {
            DataFrame documentDataFrame =
                _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as text " +
                           "union " +
                           "SELECT split('I wish c# could use case classes', ' ') as text " +
                           "union " +
                           "SELECT split('Logistic regression models are neat', ' ') as text");
            
            const string expectedInputCol = "text";
            const string expectedOutputCol = "result";
            const int expectedMinCount = 0;
            const int expectedMaxIter = 10;
            const int expectedMaxSentenceLength = 100;
            const int expectedNumPartitions = 1000;
            const int expectedSeed = 10000;
            const double expectedStepSize = 1.9;
            const int expectedVectorSize = 20;
            const int expectedWindowSize = 200;
            
            Word2Vec word2vec = new Word2Vec()
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetMinCount(expectedMinCount)
                .SetMaxIter(expectedMaxIter)
                .SetMaxSentenceLength(expectedMaxSentenceLength)
                .SetNumPartitions(expectedNumPartitions)
                .SetSeed(expectedSeed)
                .SetStepSize(expectedStepSize)
                .SetVectorSize(expectedVectorSize)
                .SetWindowSize(expectedWindowSize);
            
            Assert.Equal(word2vec.GetInputCol(), expectedInputCol);
            Assert.Equal(word2vec.GetOutputCol(), expectedOutputCol);
            Assert.Equal(word2vec.GetMinCount(), expectedMinCount);
            Assert.Equal(word2vec.GetMaxIter(), expectedMaxIter);
            Assert.Equal(word2vec.GetMaxSentenceLength(), expectedMaxSentenceLength);
            Assert.Equal(word2vec.GetNumPartitions(), expectedNumPartitions);
            Assert.Equal(word2vec.GetSeed(), expectedSeed);
            Assert.Equal(word2vec.GetStepSize(), expectedStepSize);
            Assert.Equal(word2vec.GetVectorSize(), expectedVectorSize);
            Assert.Equal(word2vec.GetWindowSize(), expectedWindowSize);
            
            Word2VecModel model = word2vec.Fit(documentDataFrame);

            DataFrame result = model.Transform(documentDataFrame);

            result.Show();

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "word2vec");
                word2vec.Save(savePath);

                Word2Vec loadedWord2Vec = Word2Vec.Load(savePath);
                Assert.Equal(word2vec.Uid(), loadedWord2Vec.Uid());
            }
        }
    }
}
