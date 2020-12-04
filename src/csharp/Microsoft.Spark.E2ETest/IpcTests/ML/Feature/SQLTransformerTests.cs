// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class SQLTransformerTests : FeatureBaseTests<SQLTransformer>
    {
        private readonly SparkSession _spark;

        public SQLTransformerTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Create a <see cref="DataFrame"/>, create a <see cref="SQLTransformer"/> and test the
        /// available methods.
        /// </summary>
        [Fact]
        public void TestSQLTransformer()
        {
            DataFrame input = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] { 0, 1.0, 3.0 }),
                    new GenericRow(new object[] { 2, 2.0, 5.0 })
                },
                new StructType(new List<StructField>
                {
                    new StructField("id", new IntegerType()),
                    new StructField("v1", new DoubleType()),
                    new StructField("v2", new DoubleType())
                }));

            string expectedUid = "theUid";
            string inputStatement = "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__";

            SQLTransformer sqlTransformer = new SQLTransformer(expectedUid)
                .SetStatement(inputStatement);

            string outputStatement = sqlTransformer.GetStatement();

            DataFrame output = sqlTransformer.Transform(input);
            StructType outputSchema = sqlTransformer.TransformSchema(input.Schema());

            Assert.Contains(output.Schema().Fields, (f => f.Name == "v3"));
            Assert.Contains(output.Schema().Fields, (f => f.Name == "v4"));
            Assert.Contains(outputSchema.Fields, (f => f.Name == "v3"));
            Assert.Contains(outputSchema.Fields, (f => f.Name == "v4"));
            Assert.Equal(inputStatement, outputStatement);

            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "SQLTransformer");
                sqlTransformer.Save(savePath);

                SQLTransformer loadedsqlTransformer = SQLTransformer.Load(savePath);
                Assert.Equal(sqlTransformer.Uid(), loadedsqlTransformer.Uid());
            }
            Assert.Equal(expectedUid, sqlTransformer.Uid());
        }
    }
}
