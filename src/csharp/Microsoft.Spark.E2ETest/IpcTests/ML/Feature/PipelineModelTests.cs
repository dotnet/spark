// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    public class PipelineModelTests : FeatureBaseTests<PipelineModel>
    {
        private readonly SparkSession _spark;

        public PipelineModelTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Create a <see cref="PipelineModel"/> and test the
        /// available methods.
        /// </summary>
        [Fact]
        public void TestPipelineModelTransform()
        {
            var expectedSplits =
                new double[] { double.MinValue, 0.0, 10.0, 50.0, double.MaxValue };

            string expectedHandle = "skip";
            string expectedUid = "uid";
            string expectedInputCol = "input_col";
            string expectedOutputCol = "output_col";

            var bucketizer = new Bucketizer(expectedUid);
            bucketizer.SetInputCol(expectedInputCol)
                .SetOutputCol(expectedOutputCol)
                .SetHandleInvalid(expectedHandle)
                .SetSplits(expectedSplits);

            var stages = new JavaTransformer[] {
                bucketizer
            };

            PipelineModel pipelineModel = new PipelineModel("randomUID", stages);

            DataFrame input = _spark.Sql("SELECT ID as input_col from range(100)");

            DataFrame output = pipelineModel.Transform(input);
            Assert.Contains(output.Schema().Fields, (f => f.Name == expectedOutputCol));

            Assert.Equal(expectedInputCol, bucketizer.GetInputCol());
            Assert.Equal(expectedOutputCol, bucketizer.GetOutputCol());
            Assert.Equal(expectedSplits, bucketizer.GetSplits());

            Assert.IsType<StructType>(pipelineModel.TransformSchema(input.Schema()));
            Assert.IsType<DataFrame>(output);
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "pipelineModel");
                pipelineModel.Save(savePath);

                PipelineModel loadedPipelineModel = PipelineModel.Load(savePath);
                Assert.Equal(pipelineModel.Uid(), loadedPipelineModel.Uid());

                string writePath = Path.Join(tempDirectory.Path, "pipelineModelWithWrite");
                pipelineModel.Write().Save(writePath);

                PipelineModel loadedPipelineModelWithRead = pipelineModel.Read().Load(writePath);
                Assert.Equal(pipelineModel.Uid(), loadedPipelineModelWithRead.Uid());
            }
        }
    }
}
