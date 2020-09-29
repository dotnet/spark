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
    public class HashingTFTests : FeatureBaseTests<HashingTF>
    {
        private readonly SparkSession _spark;

        public HashingTFTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestHashingTF()
        {
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";
            int expectedFeatures = 10;

            Assert.IsType<HashingTF>(new HashingTF());
            
            HashingTF hashingTf = new HashingTF("my-unique-id")
                .SetNumFeatures(expectedFeatures)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol);

            Assert.Equal(expectedFeatures, hashingTf.GetNumFeatures());
            Assert.Equal(expectedInputCol, hashingTf.GetInputCol());
            Assert.Equal(expectedOutputCol, hashingTf.GetOutputCol());

            DataFrame input = _spark.Sql("SELECT array('this', 'is', 'a', 'string', 'a', 'a')" +
                " as input_col");

            DataFrame output = hashingTf.Transform(input);
            DataFrame outputVector = output.Select(expectedOutputCol);
            
            Assert.Contains(expectedOutputCol, outputVector.Columns());
       
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "hashingTF");
                hashingTf.Save(savePath);
                
                HashingTF loadedHashingTf = HashingTF.Load(savePath);
                Assert.Equal(hashingTf.Uid(), loadedHashingTf.Uid());
            }

            hashingTf.SetBinary(true);
            Assert.True(hashingTf.GetBinary());
            
            TestFeatureBase(hashingTf, "numFeatures", 1000);
        }
    }
}
