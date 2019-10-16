// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Extensions.Delta.Tables;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Xunit;

namespace Microsoft.Spark.Extensions.Delta.E2ETest
{
    [Collection(Constants.DeltaTestContainerName)]
    public class DeltaTableTests
    {
        private readonly SparkSession _spark;

        public DeltaTableTests(DeltaFixture fixture)
        {
            _spark = fixture.SparkFixture.Spark;
        }

        /// <summary>
        /// Run the end-to-end scenario from the Delta Quickstart tutorial.
        /// </summary>
        /// <see cref="https://docs.delta.io/latest/quick-start.html"/>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_2)]
        public void TestTutorialScenario()
        {
            using (var tempDirectory = new TemporaryDirectory())
            {
                string path = Path.Combine(tempDirectory.Path, "delta-table");

                // Write data to a Delta table.
                DataFrame data = _spark.Range(0, 5);
                data.Write().Format("delta").Save(path);

                // Validate that data contains the the sequence [0 ... 4].
                ValidateDataFrame(Enumerable.Range(0, 5), data);

                // Create a second iteration of the table.
                data = _spark.Range(5, 10);
                data.Write().Format("delta").Mode("overwrite").Save(path);

                // Load the data into a DeltaTable object.
                var deltaTable = DeltaTable.ForPath(path);

                // Validate that deltaTable contains the the sequence [5 ... 9].
                ValidateDataFrame(Enumerable.Range(5, 5), deltaTable.ToDF());

                // Update every even value by adding 100 to it.
                deltaTable.Update(
                    condition: Functions.Expr("id % 2 == 0"),
                    set: new Dictionary<string, Column>() {
                        { "id", Functions.Expr("id + 100") }
                    });

                // Validate that deltaTable contains the the data:
                // +---+
                // | id|
                // +---+
                // |  5|
                // |  7|
                // |  9|
                // |106|
                // |108|
                // +---+
                ValidateDataFrame(
                    new List<int>() { 5, 7, 9, 106, 108 },
                    deltaTable.ToDF());

                // Delete every even value.
                deltaTable.Delete(condition: Functions.Expr("id % 2 == 0"));

                // Validate that deltaTable contains:
                // +---+
                // | id|
                // +---+
                // |  5|
                // |  7|
                // |  9|
                // +---+
                ValidateDataFrame(new List<int>() { 5, 7, 9 }, deltaTable.ToDF());

                // Upsert (merge) new data.
                DataFrame newData = _spark.Range(0, 20).As("newData").ToDF();

                deltaTable.As("oldData")
                    .Merge(newData, "oldData.id = newData.id")
                    .WhenMatched()
                    .Update(
                        new Dictionary<string, Column>() { { "id", Functions.Col("newData.id") } })
                    .WhenNotMatched()
                    .InsertExpr(new Dictionary<string, string>() { { "id", "newData.id" } })
                    .Execute();

                // Validate that the resulTable contains the the sequence [0 ... 19].
                ValidateDataFrame(Enumerable.Range(0, 20), deltaTable.ToDF());
            }
        }

        /// <summary>
        /// Run an end-to-end streaming scenario.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_2)]
        public void TestStreamingScenario()
        {
            using (var tempDirectory = new TemporaryDirectory())
            {
                // Write [0, 1, 2, 3, 4] to a Delta table.
                string sourcePath = Path.Combine(tempDirectory.Path, "source-delta-table");
                _spark.Range(0, 5).Write().Format("delta").Save(sourcePath);

                DataFrame source = _spark.ReadStream().Format("delta").Load(sourcePath);

                // Create a stream from the source DeltaTable to the sink DeltaTable.
                string sinkPath = Path.Combine(tempDirectory.Path, "sink-delta-table");
                StreamingQuery stream = source
                    .WriteStream()
                    .Format("delta")
                    .OutputMode("append")
                    .Option("checkpointLocation", Path.Combine(tempDirectory.Path, "checkpoints"))
                    .Start(sinkPath);

                // Take a short pause so that the stream has time to write to the sink.
                Thread.Sleep(TimeSpan.FromSeconds(5));
                if (!Directory.Exists(sinkPath))
                {
                    throw new Exception("Spark did not write to the sink table in time.");
                }

                DeltaTable sink = DeltaTable.ForPath(sinkPath);

                // Now read the sink DeltaTable and validate its content.
                ValidateDataFrame(Enumerable.Range(0, 5), sink.ToDF());

                // Write [5,6,7,8,9] to the source.
                _spark.Range(5, 10).Write().Format("delta").Mode("append").Save(sourcePath);

                Thread.Sleep(TimeSpan.FromSeconds(5));

                // Finally, validate that the new data made its way to the sink.
                ValidateDataFrame(Enumerable.Range(0, 10), sink.ToDF());
            }
        }

        /// <summary>
        /// Test that methods return the expected signature.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_2)]
        public void TestSignatures()
        {
            using (var tempDirectory = new TemporaryDirectory())
            {
                string path = Path.Combine(tempDirectory.Path, "delta-table");

                DataFrame rangeRate = _spark.Range(15);
                rangeRate.Write().Format("delta").Save(path);

                DeltaTable table = Assert.IsType<DeltaTable>(DeltaTable.ForPath(path));
                table = Assert.IsType<DeltaTable>(DeltaTable.ForPath(_spark, path));

                Assert.IsType<DeltaTable>(table.As("oldTable"));
                Assert.IsType<DataFrame>(table.History());
                Assert.IsType<DataFrame>(table.History(200));
                Assert.IsType<DataFrame>(table.ToDF());

                DataFrame newTable = _spark.Range(10, 15).As("newTable");
                Assert.IsType<DeltaMergeBuilder>(
                    table.Merge(newTable, Functions.Exp("oldTable.id == newTable.id")));
                DeltaMergeBuilder mergeBuilder = Assert.IsType<DeltaMergeBuilder>(
                    table.Merge(newTable, "oldTable.id == newTable.id"));

                // Validate the MergeBuilder matched signatures.
                Assert.IsType<DeltaMergeMatchedActionBuilder>(mergeBuilder.WhenMatched());
                Assert.IsType<DeltaMergeMatchedActionBuilder>(mergeBuilder.WhenMatched("id = 5"));
                DeltaMergeMatchedActionBuilder matchedActionBuilder =
                    Assert.IsType<DeltaMergeMatchedActionBuilder>(
                        mergeBuilder.WhenMatched(Functions.Expr("id = 5")));

                Assert.IsType<DeltaMergeBuilder>(
                    matchedActionBuilder.Update(new Dictionary<string, Column>()));
                Assert.IsType<DeltaMergeBuilder>(
                    matchedActionBuilder.UpdateExpr(new Dictionary<string, string>()));
                Assert.IsType<DeltaMergeBuilder>(matchedActionBuilder.UpdateAll());
                Assert.IsType<DeltaMergeBuilder>(matchedActionBuilder.Delete());

                // Validate the MergeBuilder not-matched signatures.
                Assert.IsType<DeltaMergeNotMatchedActionBuilder>(mergeBuilder.WhenNotMatched());
                Assert.IsType<DeltaMergeNotMatchedActionBuilder>(
                    mergeBuilder.WhenNotMatched("id = 5"));
                DeltaMergeNotMatchedActionBuilder notMatchedActionBuilder =
                    Assert.IsType<DeltaMergeNotMatchedActionBuilder>(
                        mergeBuilder.WhenNotMatched(Functions.Expr("id = 5")));

                Assert.IsType<DeltaMergeBuilder>(
                    notMatchedActionBuilder.Insert(new Dictionary<string, Column>()));
                Assert.IsType<DeltaMergeBuilder>(
                    notMatchedActionBuilder.InsertExpr(new Dictionary<string, string>()));
                Assert.IsType<DeltaMergeBuilder>(notMatchedActionBuilder.InsertAll());

                // Update and UpdateExpr should return void.
                table.Update(new Dictionary<string, Column>() { });
                table.Update(Functions.Expr("id % 2 == 0"), new Dictionary<string, Column>() { });
                table.UpdateExpr(new Dictionary<string, string>() { });
                table.UpdateExpr("id % 2 == 1", new Dictionary<string, string>() { });

                Assert.IsType<DataFrame>(table.Vacuum());
                Assert.IsType<DataFrame>(table.Vacuum(168));

                // Delete should return void.
                table.Delete("id > 10");
                table.Delete(Functions.Expr("id > 5"));
                table.Delete();

                // Load the table as a streaming source.
                Assert.IsType<DataFrame>(_spark.ReadStream().Format("delta").Option("path", path).Load());
                Assert.IsType<DataFrame>(_spark.ReadStream().Format("delta").Load(path));
            }
        }

        /// <summary>
        /// Validate that a tutorial DataFrame contains only the expected values.
        /// </summary>
        /// <param name="expectedValues"></param>
        /// <param name="dataFrame"></param>
        private void ValidateDataFrame(
            IEnumerable<int> expectedValues,
            DataFrame dataFrame)
        {
            Assert.Equal(expectedValues.Count(), dataFrame.Count());

            var sortedExpectedValues = new List<int>(expectedValues);
            sortedExpectedValues.Sort();

            var sortedValues = new List<int>(
                dataFrame
                    // We need to select the "id" column, otherwise Collect() won't show the
                    // updates made to the DeltaTable.
                    .Select("id")
                    .Sort("id")
                    .Collect()
                    .Select(row => Convert.ToInt32(row.Get("id"))));

            Assert.True(sortedValues.SequenceEqual(expectedValues));
        }
    }
}
