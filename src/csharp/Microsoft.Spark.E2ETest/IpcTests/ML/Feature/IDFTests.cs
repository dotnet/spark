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
    public class IDFTests
    {
        private readonly SparkSession _spark;

        public IDFTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestIDFModel()
        {
            var expectedInputCol = "rawFeatures";
            var expectedOutputCol = "features";
            var expectedDocFrequency = 100;
            
            var idf = new IDF()
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetMinDocFreq(expectedDocFrequency);
            
            Assert.Equal(expectedInputCol, idf.GetInputCol());
            Assert.Equal(expectedOutputCol, idf.GetOutputCol());
            Assert.Equal(expectedDocFrequency, idf.GetMinDocFreq());
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                var savePath = Path.Join(tempDirectory.Path, "IDF");
                idf.Save(savePath);
                var loadedIdf = IDF.Load(savePath);
                Assert.Equal(idf.Uid(), loadedIdf.Uid());
            }
        }
    }
}
