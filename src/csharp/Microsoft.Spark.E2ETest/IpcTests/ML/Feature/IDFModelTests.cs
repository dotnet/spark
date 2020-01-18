// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class IDFModelTests
    {
        private readonly SparkSession _spark;

        public IDFModelTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestIDFModel()
        {
            var expectedDocFrequency = 1980;
            var expectedInputCol = "rawFeatures";
            var expectedOutputCol = "features";
            
            DataFrame sentenceData =
                _spark.Sql("SELECT 0.0 as label, 'Hi I heard about Spark' as sentence");
            
            var tokenizer = new Tokenizer()
                .SetInputCol("sentence")
                .SetOutputCol("words");
            
            DataFrame wordsData = tokenizer.Transform(sentenceData);

            var hashingTF = new HashingTF()
                                        .SetInputCol("words")
                                        .SetOutputCol(expectedInputCol)
                                        .SetNumFeatures(20);

            DataFrame featurizedData = hashingTF.Transform(wordsData);
    
            var idf = new IDF()
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetMinDocFreq(expectedDocFrequency);
            
            var idfModel = idf.Fit(featurizedData);

            DataFrame rescaledData = idfModel.Transform(featurizedData);
            
            Assert.Equal(expectedInputCol, idfModel.GetInputCol());
            Assert.Equal(expectedOutputCol, idfModel.GetOutputCol());
            Assert.Equal(expectedDocFrequency, idfModel.GetMinDocFreq());
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                var modelPath = Path.Join(tempDirectory.Path, "ideModel");
                idfModel.Save(modelPath);
            }
        }
    }
}
