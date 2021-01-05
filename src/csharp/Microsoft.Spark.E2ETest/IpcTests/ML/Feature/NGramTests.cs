// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    /// <summary>
    /// Test suite for <see cref="NGram"/> class.
    /// </summary>
    [Collection("Spark E2E Tests")]
    public class NGramTests : FeatureBaseTests<NGram>
    {
        private readonly SparkSession _spark;

        public NGramTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test case to test the methods in <see cref="NGram"/> class.
        /// </summary>
        [Fact]
        public void TestNGram()
        {
            string expectedUid = "theUid";
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";
            int expectedN = 2;

            DataFrame input = _spark.Sql("SELECT split('Hi I heard about Spark', ' ') as input_col");

            NGram nGram = new NGram(expectedUid)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetN(expectedN);

            StructType outputSchema = nGram.TransformSchema(input.Schema());

            DataFrame output = nGram.Transform(input);

            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));
            Assert.Contains(outputSchema.Fields, (f => f.Name == expectedOutputCol));
            Assert.Equal(expectedInputCol, nGram.GetInputCol());
            Assert.Equal(expectedOutputCol, nGram.GetOutputCol());
            Assert.Equal(expectedN, nGram.GetN());

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "NGram");
                nGram.Save(savePath);

                NGram loadedNGram = NGram.Load(savePath);
                Assert.Equal(nGram.Uid(), loadedNGram.Uid());
            }

            Assert.Equal(expectedUid, nGram.Uid());

            TestFeatureBase(nGram, "inputCol", "input_col");
        }
    }
}
