// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class StringIndexerTests : FeatureBaseTests<StringIndexer>
    {
        private readonly SparkSession _spark;

        public StringIndexerTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Create a <see cref="DataFrame"/>, create a <see cref="StringIndexer"/> and test the
        /// available methods.
        /// </summary>
        [Fact]
        public void TestStringIndexer()
        {
            string expectedUid = "theUid";
            StringIndexer stringIndexer = new StringIndexer(expectedUid)
                .SetInputCol("category")
                .SetOutputCol("categoryIndex");

            Assert.Equal("category", stringIndexer.GetInputCol());
            Assert.Equal("categoryIndex", stringIndexer.GetOutputCol());
            Assert.Equal(expectedUid, stringIndexer.Uid());

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "stringIndexer");
                stringIndexer.Save(savePath);

                StringIndexer loadedstringIndexer = StringIndexer.Load(savePath);
                Assert.Equal(stringIndexer.Uid(), loadedstringIndexer.Uid());
            }
        }
    }
}
