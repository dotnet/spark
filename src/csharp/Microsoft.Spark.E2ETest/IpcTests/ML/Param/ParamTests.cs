// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.ParamTests
{
    [Collection("Spark E2E Tests")]
    public class ParamTests
    {
        private readonly SparkSession _spark;

        public ParamTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void Test()
        {
            const string expectedParent = "parent";
            const string expectedName = "name";
            const string expectedDoc = "doc";
            
            var param = new Param(expectedParent, expectedName, expectedDoc);
            
            Assert.Equal(expectedParent, param.Parent);
            Assert.Equal(expectedDoc, param.Doc);
            Assert.Equal(expectedName, param.Name);
        }
    }
}
