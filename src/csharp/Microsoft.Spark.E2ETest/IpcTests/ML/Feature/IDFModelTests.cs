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
    public class IDFModelTests : FeatureBaseTests<IDFModel>
    {
        private readonly SparkSession _spark;

        public IDFModelTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestIDFModel()
        {
            int expectedDocFrequency = 1980;
            string expectedInputCol = "rawFeatures";
            string expectedOutputCol = "features";
            
            DataFrame sentenceData =
                _spark.Sql("SELECT 0.0 as label, 'Hi I heard about Spark' as sentence");
            
            Tokenizer tokenizer = new Tokenizer()
                .SetInputCol("sentence")
                .SetOutputCol("words");
            
            DataFrame wordsData = tokenizer.Transform(sentenceData);

            HashingTF hashingTF = new HashingTF()
                .SetInputCol("words")
                .SetOutputCol(expectedInputCol)
                .SetNumFeatures(20);

            DataFrame featurizedData = hashingTF.Transform(wordsData);
    
            IDF idf = new IDF()
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetMinDocFreq(expectedDocFrequency);
            
            IDFModel idfModel = idf.Fit(featurizedData);

            DataFrame rescaledData = idfModel.Transform(featurizedData);
            Assert.Contains(expectedOutputCol, rescaledData.Columns());
            
            Assert.Equal(expectedInputCol, idfModel.GetInputCol());
            Assert.Equal(expectedOutputCol, idfModel.GetOutputCol());
            Assert.Equal(expectedDocFrequency, idfModel.GetMinDocFreq());
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                string modelPath = Path.Join(tempDirectory.Path, "idfModel");
                idfModel.Save(modelPath);

                IDFModel loadedModel = IDFModel.Load(modelPath);
                Assert.Equal(idfModel.Uid(), loadedModel.Uid());
            }
            
            TestFeatureBase(idfModel, "minDocFreq", 1000);
        }
    }
}
