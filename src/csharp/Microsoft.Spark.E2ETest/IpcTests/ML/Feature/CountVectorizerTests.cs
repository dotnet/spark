// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class CountVectorizerTests : FeatureBaseTests<CountVectorizer>
    {
        private readonly SparkSession _spark;

        public CountVectorizerTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test that we can create a CountVectorizer. Verify the standard features methods as well
        /// as load/save. 
        /// </summary>
        [Fact]
        public void TestCountVectorizer()
        {
            DataFrame input = _spark.Sql("SELECT array('hello', 'I', 'AM', 'a', 'string', 'TO', " +
                "'TOKENIZE') as input from range(100)");

            const string inputColumn = "input";
            const string outputColumn = "output";
            const double minDf = 1;
            const double minTf = 10;
            const int vocabSize = 10000;
            const bool binary = false;
            
            var countVectorizer = new CountVectorizer();
            
            countVectorizer
                .SetInputCol(inputColumn)
                .SetOutputCol(outputColumn)
                .SetMinDF(minDf)
                .SetMinTF(minTf)
                .SetVocabSize(vocabSize);
                
            Assert.IsType<CountVectorizerModel>(countVectorizer.Fit(input));
            Assert.Equal(inputColumn, countVectorizer.GetInputCol());
            Assert.Equal(outputColumn, countVectorizer.GetOutputCol());
            Assert.Equal(minDf, countVectorizer.GetMinDF());
            Assert.Equal(minTf, countVectorizer.GetMinTF());
            Assert.Equal(vocabSize, countVectorizer.GetVocabSize());
            Assert.Equal(binary, countVectorizer.GetBinary());

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "countVectorizer");
                countVectorizer.Save(savePath);
                
                CountVectorizer loadedVectorizer = CountVectorizer.Load(savePath);
                Assert.Equal(countVectorizer.Uid(), loadedVectorizer.Uid());
            }
            
            Assert.NotEmpty(countVectorizer.ExplainParams());
            Assert.NotEmpty(countVectorizer.ToString());
            
            TestFeatureBase(countVectorizer, "minDF", 0.4);
        }
        
        /// <summary>
        /// Test signatures for APIs introduced in Spark 2.4.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestSignaturesV2_4_X()
        {
            const double maxDf = 100;
            CountVectorizer countVectorizer = new CountVectorizer().SetMaxDF(maxDf);
            Assert.Equal(maxDf, countVectorizer.GetMaxDF());
        }
    }
}
