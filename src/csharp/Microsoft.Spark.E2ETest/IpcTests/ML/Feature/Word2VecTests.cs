// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class Word2VecTests : FeatureBaseTests<Word2Vec>
    {
        private readonly SparkSession _spark;

        public Word2VecTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestWord2Vec()
        {
            DataFrame documentDataFrame = _spark.Sql("SELECT split('Spark dotnet is cool', ' ')");
            
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
            
            Assert.Equal(expectedInputCol, word2vec.GetInputCol());
            Assert.Equal(expectedOutputCol, word2vec.GetOutputCol());
            Assert.Equal(expectedMinCount, word2vec.GetMinCount());
            Assert.Equal(expectedMaxIter, word2vec.GetMaxIter());
            Assert.Equal(expectedMaxSentenceLength, word2vec.GetMaxSentenceLength());
            Assert.Equal(expectedNumPartitions, word2vec.GetNumPartitions());
            Assert.Equal(expectedSeed, word2vec.GetSeed());
            Assert.Equal(expectedStepSize, word2vec.GetStepSize());
            Assert.Equal(expectedVectorSize, word2vec.GetVectorSize());
            Assert.Equal(expectedWindowSize, word2vec.GetWindowSize());
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "word2vec");
                word2vec.Save(savePath);

                Word2Vec loadedWord2Vec = Word2Vec.Load(savePath);
                Assert.Equal(word2vec.Uid(), loadedWord2Vec.Uid());
            }
            
            TestFeatureBase(word2vec, "maxIter", 2);
        }
    }
}
