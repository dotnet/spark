// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class TokenizerTests : FeatureBaseTests<Tokenizer>
    {
        private readonly SparkSession _spark;

        public TokenizerTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestTokenizer()
        {
            string expectedUid = "theUid";
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";

            DataFrame input = _spark.Sql("SELECT 'hello I AM a string TO, TOKENIZE' as input_col");

            Tokenizer tokenizer = new Tokenizer(expectedUid)
                .SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol);

            DataFrame output = tokenizer.Transform(input);

            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));
            Assert.Equal(
                new[] { "hello", "i", "am", "a", "string", "to,", "tokenize" },
                Assert.Single(output.Collect()).GetAs<string[]>(expectedOutputCol));
            Assert.Equal(expectedInputCol, tokenizer.GetInputCol());
            Assert.Equal(expectedOutputCol, tokenizer.GetOutputCol());

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "Tokenizer");
                tokenizer.Save(savePath);

                Tokenizer loadedTokenizer = Tokenizer.Load(savePath);
                Assert.Equal(tokenizer.Uid(), loadedTokenizer.Uid());
            }

            Assert.Equal(expectedUid, tokenizer.Uid());

            TestFeatureBase(tokenizer, "inputCol", "input_col");
        }

        [Fact]
        public void TestTokenizerRejectsInvalidInputAndRecovers()
        {
            Tokenizer tokenizer = new Tokenizer()
                .SetInputCol("input_col")
                .SetOutputCol("output_col");
            DataFrame invalidInput = _spark.Sql("SELECT 1 as input_col");

            Exception error = Assert.ThrowsAny<Exception>(() => tokenizer.Transform(invalidInput));
            JvmException jvmError = Assert.IsType<JvmException>(error.InnerException);
            Assert.Contains("IllegalArgumentException", jvmError.Message);
            Assert.Contains("Input type must be string", jvmError.Message);

            DataFrame validInput = _spark.Sql("SELECT 'Spark WORKS' as input_col");
            Assert.Equal(
                new[] { "spark", "works" },
                Assert.Single(tokenizer.Transform(validInput).Collect())
                    .GetAs<string[]>("output_col"));
        }
    }
}
