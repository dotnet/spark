// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.E2ETest.IpcTests.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.ML.Fpm;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Fpm
{
    [Collection("Spark E2E Tests")]
    public class FPGrowthTests : FeatureBaseTests<FPGrowth>
    {
        private readonly SparkSession _spark;

        public FPGrowthTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Create a <see cref="FPGrowth"/> and test the
        /// available methods. Test the FeatureBase methods using <see cref="FeatureBaseTests"/>.
        /// </summary>
        [Fact]
        public void TestFPGrowth()
        {

            double minSupport = 0.2;
            double minConfidence = 0.7;

            var fpGrowth = new FPGrowth();
            fpGrowth.SetMinSupport(minSupport)
                .SetMinConfidence(minConfidence);

            Assert.Equal(minSupport, fpGrowth.GetMinSupport());
            Assert.Equal(minConfidence, fpGrowth.GetMinConfidence());

            DataFrame dataFrame = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] { new string[] { "r", "z", "h", "k", "p" }}),
                },
                new StructType(new List<StructField>
                {
                    new StructField("items", new ArrayType(new StringType())),
                }));

            FPGrowthModel fpm = fpGrowth.Fit(dataFrame);
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "fpgrowth");
                fpGrowth.Save(savePath);
                
                FPGrowth loadedFPGrowth = FPGrowth.Load(savePath);
                Assert.Equal(fpGrowth.Uid(), loadedFPGrowth.Uid());
            }
            
            TestFeatureBase(fpGrowth, "itemsCol", "items");
            TestFeatureBase(fpGrowth, "numPartitions", 2);
        }

    }
}
