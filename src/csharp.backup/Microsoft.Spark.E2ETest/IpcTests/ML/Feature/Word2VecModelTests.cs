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
    public class Word2VecModelTests : FeatureBaseTests<Word2VecModel>
    {
        private readonly SparkSession _spark;

        public Word2VecModelTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestWord2VecModel()
        {
            DataFrame documentDataFrame = 
                _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as text");

            Word2Vec word2vec = new Word2Vec()
                .SetInputCol("text")
                .SetOutputCol("result")
                .SetMinCount(1);
            
            Word2VecModel model = word2vec.Fit(documentDataFrame);
            
            const int expectedSynonyms = 2;
            DataFrame synonyms = model.FindSynonyms("Hi", expectedSynonyms);

            Assert.Equal(expectedSynonyms, synonyms.Count());
            synonyms.Show();

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "word2vecModel");
                model.Save(savePath);

                Word2VecModel loadedModel = Word2VecModel.Load(savePath);
                Assert.Equal(model.Uid(), loadedModel.Uid());
            }
            
            TestFeatureBase(model, "maxIter", 2);
        }
    }
}
