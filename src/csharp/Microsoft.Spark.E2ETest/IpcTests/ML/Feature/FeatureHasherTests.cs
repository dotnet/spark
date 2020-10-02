// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class FeatureHasherTests : FeatureBaseTests<FeatureHasher>
    {
        private readonly SparkSession _spark;

        public FeatureHasherTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Create a <see cref="DataFrame"/>, create a <see cref="FeatureHasher"/> and test the
        /// available methods. Test the FeatureBase methods using <see cref="FeatureBaseTests"/>.
        /// </summary>
        [Fact]
        public void TestFeatureHasher()
        {
            DataFrame dataFrame = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] { 2.0D, true, "1", "foo" }),
                    new GenericRow(new object[] { 3.0D, false, "2", "bar" })
                },
                new StructType(new List<StructField>
                {
                    new StructField("real", new DoubleType()),
                    new StructField("bool", new BooleanType()),
                    new StructField("stringNum", new StringType()),
                    new StructField("string", new StringType())
                }));

            FeatureHasher hasher = new FeatureHasher()
                .SetInputCols(new List<string>() { "real", "bool", "stringNum", "string" })
                .SetOutputCol("features")
                .SetCategoricalCols(new List<string>() { "real", "string" })
                .SetNumFeatures(10);

            Assert.IsType<string>(hasher.GetOutputCol());
            Assert.IsType<string[]>(hasher.GetInputCols());
            Assert.IsType<string[]>(hasher.GetCategoricalCols());
            Assert.IsType<int>(hasher.GetNumFeatures());
            Assert.IsType<StructType>(hasher.TransformSchema(dataFrame.Schema()));
            Assert.IsType<DataFrame>(hasher.Transform(dataFrame));

            TestFeatureBase(hasher, "numFeatures", 1000);
        }
    }
}
