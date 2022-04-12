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
    public class PipelineTests : FeatureBaseTests<Pipeline>
    {
        private readonly SparkSession _spark;

        public PipelineTests(SparkFixture fixture) : base(fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Create a <see cref="Pipeline"/> and test the
        /// available methods. Test the FeatureBase methods 
        /// using <see cref="TestFeatureBase"/>.
        /// </summary>
        [Fact]
        public void TestPipeline()
        {
            var stages = new JavaPipelineStage[] {
                new Bucketizer(),
                new CountVectorizer()
            };

            Pipeline pipeline = new Pipeline()
                .SetStages(stages);
            JavaPipelineStage[] returnStages = pipeline.GetStages();
            
            Assert.Equal(stages[0].Uid(), returnStages[0].Uid());
            Assert.Equal(stages[0].ToString(), returnStages[0].ToString());
            Assert.Equal(stages[1].Uid(), returnStages[1].Uid());
            Assert.Equal(stages[1].ToString(), returnStages[1].ToString());
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "pipeline");
                pipeline.Save(savePath);

                Pipeline loadedPipeline = Pipeline.Load(savePath);
                Assert.Equal(pipeline.Uid(), loadedPipeline.Uid());
            }
            
            TestFeatureBase(pipeline, "stages", stages);
        }

        /// <summary>
        /// Create a <see cref="Pipeline"/> and test the
        /// fit and read/write methods.
        /// </summary>
        [Fact]
        public void TestPipelineFit()
        {
            DataFrame input = _spark.Sql("SELECT array('hello', 'I', 'AM', 'a', 'string', 'TO', " +
                "'TOKENIZE') as input from range(100)");

            const string inputColumn = "input";
            const string outputColumn = "output";
            const double minDf = 1;
            const double minTf = 10;
            const int vocabSize = 10000;
            
            CountVectorizer countVectorizer = new CountVectorizer()
                .SetInputCol(inputColumn)
                .SetOutputCol(outputColumn)
                .SetMinDF(minDf)
                .SetMinTF(minTf)
                .SetVocabSize(vocabSize);

            var stages = new JavaPipelineStage[] {
                countVectorizer
            };

            Pipeline pipeline = new Pipeline().SetStages(stages);
            PipelineModel pipelineModel = pipeline.Fit(input);
            
            DataFrame output = pipelineModel.Transform(input);

            Assert.IsType<StructType>(pipelineModel.TransformSchema(input.Schema()));
            Assert.IsType<DataFrame>(output);
            
            using (var tempDirectory = new TemporaryDirectory())
            {
                string savePath = Path.Join(tempDirectory.Path, "pipeline");
                pipeline.Save(savePath);

                Pipeline loadedPipeline = Pipeline.Load(savePath);
                Assert.Equal(pipeline.Uid(), loadedPipeline.Uid());

                string writePath = Path.Join(tempDirectory.Path, "pipelineWithWrite");
                pipeline.Write().Save(writePath);

                Pipeline loadedPipelineWithRead = pipeline.Read().Load(writePath);
                Assert.Equal(pipeline.Uid(), loadedPipelineWithRead.Uid());
            }
            
            TestFeatureBase(pipeline, "stages", stages);
        }
    }
}
