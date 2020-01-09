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
    public class TokenizerTests
    {
        private readonly SparkSession _spark;

        public TokenizerTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestTokenizer()
        {
            Tokenizer Tokenizer = new Tokenizer("uid")
                .SetInputCol("input_col")
                .SetOutputCol("output_col");

            Assert.Equal("uid", Tokenizer.Uid());

            DataFrame input = _spark.Sql("SELECT 'hello I AM a string TO, TOKENIZE' as input_col" + 
                                                " from range(100)");

            DataFrame output = Tokenizer.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == "output_col"));
        }
    }
}
