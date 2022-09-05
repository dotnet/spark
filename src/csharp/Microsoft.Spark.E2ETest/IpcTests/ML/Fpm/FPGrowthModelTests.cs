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
    public class FPGrowthModelTests : FeatureBaseTests<FPGrowthModel>
    {
        private readonly SparkSession _spark;

        public FPGrowthModelTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Create a <see cref="FPGrowthModel"/> and test the
        /// available methods. Test the FeatureBase methods using <see cref="FeatureBaseTests"/>.
        /// </summary>
        [Fact]
        public void TestFPGrowthModel()
        {
            var fpGrowth = new FPGrowth();
            fpGrowth.SetMinSupport(0.2)
                .SetMinConfidence(0.7);

            DataFrame dataFrame = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] { new string[] { "r", "z", "h", "k", "p" }}),
                    new GenericRow(new object[] { new string[] { "z", "y", "x", "w", "v", "u", "t", "s" }}),
                    new GenericRow(new object[] { new string[] { "s", "x", "o", "n", "r" }}),
                    new GenericRow(new object[] { new string[] { "x", "z", "y", "m", "t", "s", "q", "e" }}),
                    new GenericRow(new object[] { new string[] { "z" }}),
                    new GenericRow(new object[] { new string[] { "x", "z", "y", "r", "q", "t", "p" }}),
                },
                new StructType(new List<StructField>
                {
                    new StructField("items", new ArrayType(new StringType())),
                }));

            FPGrowthModel fpm = fpGrowth.Fit(dataFrame);
            fpm.SetPredictionCol("newPrediction");
            Assert.Equal(0.2, fpm.GetMinSupport());
            Assert.Equal(0.7, fpm.GetMinConfidence());
            
            DataFrame newData = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] { new string[] {"t", "s"}})
                },
                new StructType(new List<StructField>
                {
                    new StructField("items", new ArrayType(new StringType())),
                })
            );
            var prediction = TypeConverter.ConvertTo<string[]>(
                fpm.Transform(newData).Select("newPrediction").First().Values[0]);
            Array.Sort(prediction);
            Assert.Equal(prediction, new string[]{ "x", "y", "z"});
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "fpm");
                fpm.Save(savePath);
                
                FPGrowthModel loadedFPGrowthModel = FPGrowthModel.Load(savePath);
                Assert.Equal(fpm.Uid(), loadedFPGrowthModel.Uid());
                var newPrediction = TypeConverter.ConvertTo<string[]>(loadedFPGrowthModel
                    .Transform(newData).Select("newPrediction").First().Values[0]);
                Array.Sort(newPrediction);
                Assert.Equal(new string[]{ "x", "y", "z"}, newPrediction);
            }
            
            TestFeatureBase(fpm, "itemsCol", "items");
            TestFeatureBase(fpm, "minConfidence", 0.7);
            TestFeatureBase(fpm, "minSupport", 0.2);
            TestFeatureBase(fpm, "numPartitions", 2);
            TestFeatureBase(fpm, "predictionCol", "prediction");
        }

    }
}
