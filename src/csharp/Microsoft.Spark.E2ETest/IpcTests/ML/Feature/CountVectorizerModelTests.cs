// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class CountVectorizerModelTests
    {
        private readonly SparkSession _spark;

        public CountVectorizerModelTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void Test_CountVectorizerModel()
        {
            DataFrame input = _spark.Sql("SELECT array('hello', 'I', 'AM', 'a', 'string', 'TO', " +
                                         "'TOKENIZE') as input from range(100)");

            const string inputColumn = "input";
            const string outputColumn = "output";
            const double minTf = 10.0;
            const bool binary = false;
            
            List<string> vocabulary = new List<string>()
            {
                "hello",
                "I",
                "AM",
                "TO",
                "TOKENIZE"
            };
            
            var countVectorizerModel = new CountVectorizerModel(vocabulary);
            
            Assert.IsType<CountVectorizerModel>(new CountVectorizerModel("my-uid", vocabulary));
            
            countVectorizerModel = countVectorizerModel
                .SetInputCol(inputColumn)
                .SetOutputCol(outputColumn)
                .SetMinTF(minTf)
                .SetBinary(binary);
            
            Assert.Equal(inputColumn, countVectorizerModel.GetInputCol());
            Assert.Equal(outputColumn, countVectorizerModel.GetOutputCol());
            Assert.Equal(minTf, countVectorizerModel.GetMinTF());
            Assert.Equal(binary, countVectorizerModel.GetBinary());
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "countVectorizerModel");
                countVectorizerModel.Save(savePath);
                
                CountVectorizerModel loadedModel = CountVectorizerModel.Load(savePath);
                Assert.Equal(countVectorizerModel.Uid(), loadedModel.Uid());
            }

            Assert.IsType<int>(countVectorizerModel.GetVocabSize());
            Assert.NotEmpty(countVectorizerModel.ExplainParams());
            Assert.NotEmpty(countVectorizerModel.ToString());
        } 
    }
}
