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
    public class HashingTFTests
    {
        private readonly SparkSession _spark;

        public HashingTFTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestHashingTF()
        {
            HashingTF HashingTF = new HashingTF(100)
                .SetNumFeatures(10)
                .SetInputCol("input_col")
                .SetOutputCol("output_col");

            Assert.Equal(10, HashingTF.GetNumFeatures());

            DataFrame input = _spark.Sql("SELECT array('this', 'is', 'a', 'string', 'a', 'a')" + 
                                            " as input_col");

            DataFrame output = HashingTF.Transform(input);
        }
    }
}
