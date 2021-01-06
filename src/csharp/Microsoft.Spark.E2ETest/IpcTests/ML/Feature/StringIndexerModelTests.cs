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
    public class StringIndexerModelTests : FeatureBaseTests<StringIndexerModel>
    {
        private readonly SparkSession _spark;

        public StringIndexerModelTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Create a <see cref="DataFrame"/>, create a <see cref="StringIndexerModel"/> and test the
        /// available methods.
        /// </summary>
        [Fact]
        public void TestStringIndexerModel()
        {
            DataFrame input = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                new GenericRow(new object[] { (0, "a") }),
                new GenericRow(new object[] { (1, "b") }),
                new GenericRow(new object[] { (2, "c") }),
                new GenericRow(new object[] { (3, "a") }),
                new GenericRow(new object[] { (4, "a") }),
                new GenericRow(new object[] { (5, "c") })
                },
                new StructType(new List<StructField>
                {
                    new StructField("id", new IntegerType()),
                    new StructField("category", new StringType())
                }));

            string expectedUid = "theUid";
            StringIndexer stringIndexer = new StringIndexer(expectedUid)
                .SetInputCol("category")
                .SetOutputCol("categoryIndex");

            StringIndexerModel stringIndexerModel = stringIndexer.Fit(input);
            DataFrame transformedDF = stringIndexerModel.Transform(input);
            List<Row> observed = transformedDF.Select("category", new string[] { "categoryIndex" })
                .Collect().ToList();
            List<Row> expected = new List<Row>
            {
                new Row(new GenericRow(new object[] {("a", "0") })),
                new Row(new GenericRow(new object[] {("c", "1") })),
                new Row(new GenericRow(new object[] {("b", "2") }))
            };

            Assert.Equal(observed, expected);
            Assert.Equal("category", stringIndexer.GetInputCol());
            Assert.Equal("categoryIndex", stringIndexer.GetOutputCol());
            Assert.Equal(expectedUid, stringIndexer.Uid());

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "stringIndexerModel");
                stringIndexerModel.Save(savePath);

                StringIndexerModel loadedModel = StringIndexerModel.Load(savePath);
                Assert.Equal(stringIndexerModel.Uid(), loadedModel.Uid());
            }
        }
    }
}
