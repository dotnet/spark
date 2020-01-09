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
            IDF idf = new IDF("uid")
                .SetMinDocFreq(2)
                .SetInputCol("input_col")
                .SetOutputCol("output_col");

            Assert.Equal("uid", idf.Uid());

            DataFrame input = _spark.Sql("SELECT array('this', 'is', 'a', 'string', 'a', 'a')" + 
                                            " as input_col");

            IDFModel model = idf.Fit(input);
            model.Transform(input);

        }
    }
}
