// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Feature
{
    [Collection("Spark E2E Tests")]
    [Trait("Category", "ML")]
    public class MLCompatibilityTests
    {
        private const string PipelineUid = "orders-features";
        private readonly SparkSession _spark;

        public MLCompatibilityTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestPipelineModelPersistence(bool useWriter)
        {
            // The runner uses two separate testhost/JVM processes; normal suite runs
            // exercise the same assertions as a self-contained round trip.
            string phase = Environment.GetEnvironmentVariable("DOTNET_SPARK_ML_PERSISTENCE_PHASE");
            string root = Environment.GetEnvironmentVariable("DOTNET_SPARK_ML_MODEL_PATH");
            Assert.True(
                (phase == null && root == null) ||
                ((phase == "save" || phase == "load") &&
                    !string.IsNullOrWhiteSpace(root) && Path.IsPathFullyQualified(root)),
                "ML persistence requires both a save/load phase and an absolute model path.");

            string mode = phase == "save" ? "save" : phase == "load" ? "load" : "self-contained";
            string traceContext = $"ml-pipeline-persistence useWriter={useWriter} mode={mode}";
            IpcDebugTrace.Write($"{traceContext} phase=begin");
            using (var tempDirectory = phase == null ? new TemporaryDirectory() : null)
            {
                string path = Path.Combine(
                    root ?? tempDirectory.Path,
                    useWriter ? "pipeline-model-write" : "pipeline-model-save");

                if (phase != "load")
                {
                    IpcDebugTrace.Write($"{traceContext} phase=fit-begin");
                    PipelineModel model = FitPipeline();
                    IpcDebugTrace.Write($"{traceContext} phase=fit-end");
                    IpcDebugTrace.Write($"{traceContext} phase=collect-before-save-begin");
                    AssertModelOutput(model);
                    IpcDebugTrace.Write($"{traceContext} phase=collect-before-save-end");
                    IpcDebugTrace.Write($"{traceContext} phase=save-begin");
                    if (useWriter)
                    {
                        model.Write().Session(_spark).Save(path);
                    }
                    else
                    {
                        model.Save(path);
                    }
                    IpcDebugTrace.Write($"{traceContext} phase=save-end");

                    if (phase == "save")
                    {
                        IpcDebugTrace.Write($"{traceContext} phase=end");
                        return;
                    }
                }

                // Do not Fit in the load phase: all learned state must come from disk.
                IpcDebugTrace.Write($"{traceContext} phase=load-begin");
                PipelineModel loaded = useWriter
                    ? new PipelineModel("reader", Array.Empty<JavaTransformer>())
                        .Read().Session(_spark).Load(path)
                    : PipelineModel.Load(path);
                IpcDebugTrace.Write($"{traceContext} phase=load-end");
                IpcDebugTrace.Write($"{traceContext} phase=collect-after-load-begin");
                AssertModelOutput(loaded);
                IpcDebugTrace.Write($"{traceContext} phase=collect-after-load-end");
            }
            IpcDebugTrace.Write($"{traceContext} phase=end");
        }

        [Fact]
        public void TestModelPersistenceFailures()
        {
            using (var tempDirectory = new TemporaryDirectory())
            {
                string missingPath = Path.Combine(tempDirectory.Path, "missing-model");
                Exception missing = Assert.Throws<Exception>(() => PipelineModel.Load(missingPath));
                Assert.IsType<JvmException>(missing.InnerException);
                Assert.Contains("missing-model", missing.InnerException.Message);

                PipelineModel model = FitPipeline();
                string path = Path.Combine(tempDirectory.Path, "model");
                model.Save(path);

                Exception duplicate = Assert.Throws<Exception>(() => model.Save(path));
                Assert.IsType<JvmException>(duplicate.InnerException);
                Assert.Contains("already exists", duplicate.InnerException.Message);

                // A rejected second save must not damage the first model.
                AssertModelOutput(PipelineModel.Load(path));
                model.Write().Overwrite().Save(path);
                AssertModelOutput(PipelineModel.Load(path));
            }
        }

        private PipelineModel FitPipeline()
        {
            // Distinct term frequencies make vocabulary order deterministic: red, blue, green.
            DataFrame training = _spark.Sql(
                "SELECT * FROM VALUES (0, 'Red red blue'), (1, 'red green'), " +
                "(2, 'blue') AS training(id, text)");
            var stages = new JavaPipelineStage[]
            {
                new Tokenizer().SetInputCol("text").SetOutputCol("words"),
                new CountVectorizer().SetInputCol("words").SetOutputCol("counts")
                    .SetVocabSize(3).SetMinDF(1.0).SetMinTF(1.0),
                new IDF().SetInputCol("counts").SetOutputCol("features")
                    .SetMinDocFreq(0)
            };
            var pipeline = new Pipeline(PipelineUid).SetStages(stages);
            JavaPipelineStage[] actualStages = pipeline.GetStages();
            Assert.Equal(stages.Select(stage => stage.Uid()),
                actualStages.Select(stage => stage.Uid()));
            Assert.IsType<Tokenizer>(actualStages[0]);
            Assert.IsType<CountVectorizer>(actualStages[1]);
            Assert.IsType<IDF>(actualStages[2]);
            return pipeline.Fit(training);
        }

        private void AssertModelOutput(PipelineModel model)
        {
            Assert.Equal(PipelineUid, model.Uid());
            DataFrame input = _spark.Sql(
                "SELECT * FROM VALUES (0, 'green red green unknown'), " +
                "(1, 'blue red'), (2, '') AS inference(id, text)");
            Row[] rows = model.Transform(input)
                .Select("id", "words", "counts", "features").OrderBy("id").Collect().ToArray();
            Assert.Equal(new[] { 0, 1, 2 }, rows.Select(row => row.GetAs<int>("id")));
            Assert.Equal(new[] { "green", "red", "green", "unknown" },
                rows[0].GetAs<string[]>("words"));

            double commonIdf = Math.Log(4.0 / 3.0);
            var expectedCounts = new[]
            {
                new[] { 1.0, 0.0, 2.0 },
                new[] { 1.0, 1.0, 0.0 },
                new[] { 0.0, 0.0, 0.0 }
            };
            var expectedFeatures = new[]
            {
                new[] { commonIdf, 0.0, 2.0 * Math.Log(2.0) },
                new[] { commonIdf, commonIdf, 0.0 },
                new[] { 0.0, 0.0, 0.0 }
            };
            for (int i = 0; i < rows.Length; ++i)
            {
                AssertSparseVector(rows[i].GetAs<Row>("counts"), expectedCounts[i]);
                AssertSparseVector(rows[i].GetAs<Row>("features"), expectedFeatures[i]);
            }
        }

        private static void AssertSparseVector(Row vector, double[] expected)
        {
            Assert.Equal(0, vector.GetAs<int>("type"));
            Assert.Equal(expected.Length, vector.GetAs<int>("size"));
            int[] indices = vector.GetAs<int[]>("indices");
            double[] values = vector.GetAs<double[]>("values");
            Assert.Equal(indices.Length, values.Length);
            Assert.Equal(indices.Distinct().OrderBy(index => index), indices);

            var actual = new double[expected.Length];
            for (int i = 0; i < indices.Length; ++i)
            {
                Assert.InRange(indices[i], 0, expected.Length - 1);
                actual[indices[i]] = values[i];
            }

            for (int i = 0; i < expected.Length; ++i)
            {
                Assert.Equal(expected[i], actual[i], precision: 10);
            }
        }
    }
}
