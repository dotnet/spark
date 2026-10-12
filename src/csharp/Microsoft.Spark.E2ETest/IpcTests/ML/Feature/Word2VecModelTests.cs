// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using System.Linq;
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
                .SetVectorSize(3)
                .SetNumPartitions(1)
                .SetSeed(42)
                .SetMinCount(1);

            Word2VecModel model = word2vec.Fit(documentDataFrame);

            const int expectedSynonyms = 2;
            DataFrame synonyms = model.FindSynonyms("Hi", expectedSynonyms);

            Assert.Equal(expectedSynonyms, synonyms.Count());
            synonyms.Show();

            Row vector = model.Transform(documentDataFrame).Select("result")
                .Collect().Single().GetAs<Row>("result");
            Assert.Equal(1, vector.GetAs<int>("type"));
            double[] expected = vector.GetAs<double[]>("values");
            Assert.Equal(3, expected.Length);
            Assert.All(expected, value => Assert.True(double.IsFinite(value)));
            Assert.Contains(expected, value => value != 0.0);

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "word2vecModel");
                model.Save(savePath);

                Word2VecModel loadedModel = Word2VecModel.Load(savePath);
                Assert.Equal(model.Uid(), loadedModel.Uid());

                Row loadedVector = loadedModel.Transform(documentDataFrame).Select("result")
                    .Collect().Single().GetAs<Row>("result");
                Assert.Equal(1, loadedVector.GetAs<int>("type"));
                double[] actual = loadedVector.GetAs<double[]>("values");
                Assert.Equal(expected.Length, actual.Length);
                for (int i = 0; i < expected.Length; ++i)
                {
                    Assert.Equal(expected[i], actual[i], precision: 10);
                }
            }

            TestFeatureBase(model, "maxIter", 2);
        }
    }
}
