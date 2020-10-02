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
    public class IDFTests : FeatureBaseTests<IDF>
    {
        private readonly SparkSession _spark;

        public IDFTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestIDFModel()
        {
            string expectedInputCol = "rawFeatures";
            string expectedOutputCol = "features";
            int expectedDocFrequency = 100;
            
            IDF idf = new IDF()
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetMinDocFreq(expectedDocFrequency);
            
            Assert.Equal(expectedInputCol, idf.GetInputCol());
            Assert.Equal(expectedOutputCol, idf.GetOutputCol());
            Assert.Equal(expectedDocFrequency, idf.GetMinDocFreq());
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "IDF");
                idf.Save(savePath);
                
                IDF loadedIdf = IDF.Load(savePath);
                Assert.Equal(idf.Uid(), loadedIdf.Uid());
            }
            
            TestFeatureBase(idf, "minDocFreq", 1000);
        }
    }
}
