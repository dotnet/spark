// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Spark.E2ETest.IpcTests;
using Microsoft.Spark.E2ETest.UdfTests;
using Microsoft.Spark.Interop.Ipc;
using Xunit;
using Xunit.Abstractions;

namespace Microsoft.Spark.E2ETest
{
    /// <summary>
    /// Opt-in debug-branch experiment: reuse the real tests in one Spark session.
    /// Each repetition remains a separate test case so VSTest detects a stalled case.
    /// </summary>
    [Collection("Spark E2E Tests")]
    [Trait("Category", "HangStress")]
    public class SparkHangStressTests
    {
        private readonly SparkFixture _fixture;
        private readonly ITestOutputHelper _output;

        public SparkHangStressTests(SparkFixture fixture, ITestOutputHelper output)
        {
            _fixture = fixture;
            _output = output;
        }

        public static IEnumerable<object[]> Iterations()
        {
            string value = Environment.GetEnvironmentVariable("DOTNET_SPARK_STRESS_ITERATIONS");
            int count = 1;
            if (value != null && (!int.TryParse(value, out count) || count < 1 || count > 1000))
            {
                throw new ArgumentException("Stress iterations must be between 1 and 1000.");
            }

            for (int iteration = 1; iteration <= count; ++iteration)
            {
                yield return new object[] { iteration };
            }
        }

        [StressTheory]
        [MemberData(nameof(Iterations))]
        public void AvroAfterSelectedPrefix(int iteration)
        {
            string scenario = Environment.GetEnvironmentVariable("DOTNET_SPARK_STRESS_SCENARIO");
            switch (scenario)
            {
                case "avro":
                case "avro-cold":
                    break;
                case "udf":
                    RunStep(iteration, "date-udf", () =>
                        new UdfSimpleTypesTests(_fixture).TestUdfWithDateType());
                    RunStep(iteration, "timestamp-udf", () =>
                        new UdfSimpleTypesTests(_fixture).TestUdfWithTimestampType());
                    break;
                case "udf-sequence":
                    RunStep(iteration, "return-date", () =>
                        new UdfSimpleTypesTests(_fixture).TestUdfWithReturnAsDateType());
                    RunStep(iteration, "return-timestamp", () =>
                        new UdfSimpleTypesTests(_fixture).TestUdfWithReturnAsTimestampType());
                    RunStep(iteration, "duplicate-timestamps", () =>
                        new UdfSimpleTypesTests(_fixture).TestUdfWithDuplicateTimestamps());
                    RunStep(iteration, "define-udf-threads", () =>
                        new UdfSimpleTypesTests(_fixture).TestUdfWithMultipleThreads());
                    RunStep(iteration, "date-udf", () =>
                        new UdfSimpleTypesTests(_fixture).TestUdfWithDateType());
                    RunStep(iteration, "timestamp-udf", () =>
                        new UdfSimpleTypesTests(_fixture).TestUdfWithTimestampType());
                    break;
                case "worker-failure":
                    var compatibility = new Spark40CompatibilityTests(_fixture, _output);
                    RunStep(iteration, "scalar-worker-failure", () =>
                        compatibility.ScalarUdfWorkerFailureAllowsSubsequentTask());
                    RunStep(iteration, "rdd-worker-failure", () =>
                        compatibility.RddNonUdfWorkerFailurePropagates());
                    break;
                case "avro-errors":
                    var avro = new AvroFunctionsTests(_fixture);
                    RunStep(iteration, "invalid-encode-schema", () =>
                        avro.InvalidSchemaPropagatesJvmError(true));
                    RunStep(iteration, "invalid-decode-schema", () =>
                        avro.InvalidSchemaPropagatesJvmError(false));
                    RunStep(iteration, "empty-avro", () => avro.MalformedDataHonorsDecodeMode(""));
                    RunStep(iteration, "truncated-avro", () => avro.MalformedDataHonorsDecodeMode("80"));
                    break;
                default:
                    throw new ArgumentException(
                        "Select avro, avro-cold, udf, udf-sequence, worker-failure or avro-errors.");
            }

            // Cold runs use cache.maxEntries=1 at JVM startup. Check the real metric:
            // repeating identical plans with the default cache barely exercises Janino.
            long compiledBefore = scenario == "avro-cold" ? CompilationCount() : 0;
            RunStep(iteration, "nullable-avro", () =>
                new AvroFunctionsTests(_fixture).NullableValuesRespectExplicitSchema());
            if (scenario == "avro-cold")
            {
                long compiled = CompilationCount() - compiledBefore;
                IpcDebugTrace.Write($"stress iteration={iteration} codegen-compilations={compiled}");
                _output.WriteLine($"iteration={iteration} codegen-compilations={compiled}");
                Assert.True(compiled >= 2,
                    $"Expected at least two fresh compilations, but observed {compiled}.");
            }
        }

        private long CompilationCount()
        {
            var histogram = (JvmObjectReference)_fixture.Jvm.CallStaticJavaMethod(
                "org.apache.spark.metrics.source.CodegenMetrics", "METRIC_COMPILATION_TIME");
            return (long)histogram.Invoke("getCount");
        }

        private void RunStep(int iteration, string phase, Action action)
        {
            IpcDebugTrace.Write($"stress iteration={iteration} phase={phase} begin");
            var elapsed = Stopwatch.StartNew();
            action();
            IpcDebugTrace.Write($"stress iteration={iteration} phase={phase} end elapsed-ms={elapsed.ElapsedMilliseconds}");
            _output.WriteLine($"iteration={iteration} phase={phase} elapsed-ms={elapsed.ElapsedMilliseconds}");
        }

        public sealed class StressTheoryAttribute : TheoryAttribute
        {
            public StressTheoryAttribute()
            {
                if (Environment.GetEnvironmentVariable("DOTNET_SPARK_STRESS_ENABLE") != "1")
                {
                    Skip = "Set DOTNET_SPARK_STRESS_ENABLE=1 for the local hang experiment.";
                }
            }
        }
    }
}
