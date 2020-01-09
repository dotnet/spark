// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
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
            DataFrame sentenceData =
                _spark.Sql("SELECT 0.0 as label, 'Hi I heard about Spark' as sentence");
            Tokenizer tokenizer = new Tokenizer().SetInputCol("sentence").SetOutputCol("words");
            DataFrame wordsData = tokenizer.Transform(sentenceData);

            HashingTF hashingTF = new HashingTF()
                .SetInputCol("words").SetOutputCol("rawFeatures").SetNumFeatures(20);

            DataFrame featurizedData = hashingTF.Transform(wordsData);

            IDF idf = new IDF().SetInputCol("rawFeatures").SetOutputCol("features");
            IDFModel idfModel = idf.Fit(featurizedData);

            DataFrame rescaledData = idfModel.Transform(featurizedData);
            
        }
    }
}
