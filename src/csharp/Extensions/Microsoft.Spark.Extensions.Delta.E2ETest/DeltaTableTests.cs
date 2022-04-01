// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Extensions.Delta.Tables;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
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
        ///
        /// Delta 0.8.0 is not compatible with Spark 3.1.1
        /// Disable Delta tests that have code paths that create an
        /// `org.apache.spark.sql.catalyst.expressions.Alias` object.
        [SkipIfSparkVersionIsNotInRange(Versions.V2_4_2, Versions.V3_1_1)]
        public void TestTutorialScenario()
        {
            using var tempDirectory = new TemporaryDirectory();
            string path = Path.Combine(tempDirectory.Path, "delta-table");

            // Write data to a Delta table.
            DataFrame data = _spark.Range(0, 5);
            data.Write().Format("delta").Save(path);

            // Validate that data contains the the sequence [0 ... 4].
            ValidateRangeDataFrame(Enumerable.Range(0, 5), data);

            // Create a second iteration of the table.
            data = _spark.Range(5, 10);
            data.Write().Format("delta").Mode("overwrite").Save(path);

            // Load the data into a DeltaTable object.
            DeltaTable deltaTable = DeltaTable.ForPath(path);

            // Validate that deltaTable contains the the sequence [5 ... 9].
            ValidateRangeDataFrame(Enumerable.Range(5, 5), deltaTable.ToDF());

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
            ValidateRangeDataFrame(
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
            ValidateRangeDataFrame(new List<int>() { 5, 7, 9 }, deltaTable.ToDF());

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
            ValidateRangeDataFrame(Enumerable.Range(0, 20), deltaTable.ToDF());
        }

        /// <summary>
        /// Run an end-to-end streaming scenario.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_2)]
        public void TestStreamingScenario()
        {
            using var tempDirectory = new TemporaryDirectory();
            // Write [0, 1, 2, 3, 4] to a Delta table.
            string sourcePath = Path.Combine(tempDirectory.Path, "source-delta-table");
            _spark.Range(0, 5).Write().Format("delta").Save(sourcePath);

            // Create a stream from the source DeltaTable to the sink DeltaTable.
            // To make the test synchronous and deterministic, we will use a series of 
            // "one-time micro-batch" triggers.
            string sinkPath = Path.Combine(tempDirectory.Path, "sink-delta-table");
            DataStreamWriter dataStreamWriter = _spark
                .ReadStream()
                .Format("delta")
                .Load(sourcePath)
                .WriteStream()
                .Format("delta")
                .OutputMode("append")
                .Option("checkpointLocation", Path.Combine(tempDirectory.Path, "checkpoints"));

            // Trigger the first stream batch
            dataStreamWriter.Trigger(Trigger.Once()).Start(sinkPath).AwaitTermination();

            // Now read the sink DeltaTable and validate its content.
            DeltaTable sink = DeltaTable.ForPath(sinkPath);
            ValidateRangeDataFrame(Enumerable.Range(0, 5), sink.ToDF());

            // Write [5,6,7,8,9] to the source and trigger another stream batch.
            _spark.Range(5, 10).Write().Format("delta").Mode("append").Save(sourcePath);
            dataStreamWriter.Trigger(Trigger.Once()).Start(sinkPath).AwaitTermination();

            // Finally, validate that the new data made its way to the sink.
            ValidateRangeDataFrame(Enumerable.Range(0, 10), sink.ToDF());
        }

        /// <summary>
        /// Test <c>DeltaTable.IsDeltaTable()</c> API.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_2)]
        public void TestIsDeltaTable()
        {
            using var tempDirectory = new TemporaryDirectory();
            // Save the same data to a DeltaTable and to Parquet.
            DataFrame data = _spark.Range(0, 5);
            string parquetPath = Path.Combine(tempDirectory.Path, "parquet-data");
            data.Write().Parquet(parquetPath);
            string deltaTablePath = Path.Combine(tempDirectory.Path, "delta-table");
            data.Write().Format("delta").Save(deltaTablePath);

            Assert.False(DeltaTable.IsDeltaTable(parquetPath));
            Assert.False(DeltaTable.IsDeltaTable(_spark, parquetPath));

            Assert.True(DeltaTable.IsDeltaTable(deltaTablePath));
            Assert.True(DeltaTable.IsDeltaTable(_spark, deltaTablePath));
        }

        /// <summary>
        /// Test <c>DeltaTable.ConvertToDelta()</c> API.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_2)]
        public void TestConvertToDelta()
        {
            string partitionColumnName = "id_plus_one";
            DataFrame data = _spark.Range(0, 5).Select(
                Functions.Col("id"),
                Functions.Expr($"(`id` + 1) AS `{partitionColumnName}`"));

            // Run the same test on the different overloads of DeltaTable.ConvertToDelta().
            void testWrapper(
                DataFrame dataFrame,
                Func<string, DeltaTable> convertToDelta,
                string partitionColumn = null)
            {
                using var tempDirectory = new TemporaryDirectory();
                string path = Path.Combine(tempDirectory.Path, "parquet-data");
                DataFrameWriter dataWriter = dataFrame.Write();

                if (!string.IsNullOrEmpty(partitionColumn))
                {
                    dataWriter = dataWriter.PartitionBy(partitionColumn);
                }

                dataWriter.Parquet(path);

                Assert.False(DeltaTable.IsDeltaTable(path));

                string identifier = $"parquet.`{path}`";
                DeltaTable convertedDeltaTable = convertToDelta(identifier);

                ValidateRangeDataFrame(Enumerable.Range(0, 5), convertedDeltaTable.ToDF());
                Assert.True(DeltaTable.IsDeltaTable(path));
            }

            testWrapper(data, identifier => DeltaTable.ConvertToDelta(_spark, identifier));
            testWrapper(
                data.Repartition(Functions.Col(partitionColumnName)),
                identifier => DeltaTable.ConvertToDelta(
                    _spark,
                    identifier,
                    $"{partitionColumnName} bigint"),
                partitionColumnName);
            testWrapper(
                data.Repartition(Functions.Col(partitionColumnName)),
                identifier => DeltaTable.ConvertToDelta(
                    _spark,
                    identifier,
                    new StructType(new[]
                    {
                        new StructField(partitionColumnName, new IntegerType())
                    })),
                partitionColumnName);
        }

        /// <summary>
        /// Test that methods return the expected signature.
        /// </summary>
        ///
        /// Delta 0.8.0 is not compatible with Spark 3.1.1
        /// Disable Delta tests that have code paths that create an
        /// `org.apache.spark.sql.catalyst.expressions.Alias` object.
        [SkipIfSparkVersionIsNotInRange(Versions.V2_4_2, Versions.V3_1_1)]
        public void TestSignaturesV2_4_X()
        {
            using var tempDirectory = new TemporaryDirectory();
            string path = Path.Combine(tempDirectory.Path, "delta-table");

            DataFrame rangeRate = _spark.Range(15);
            rangeRate.Write().Format("delta").Save(path);

            DeltaTable table = Assert.IsType<DeltaTable>(DeltaTable.ForPath(path));
            table = Assert.IsType<DeltaTable>(DeltaTable.ForPath(_spark, path));

            Assert.IsType<bool>(DeltaTable.IsDeltaTable(_spark, path));
            Assert.IsType<bool>(DeltaTable.IsDeltaTable(path));

            Assert.IsType<DeltaTable>(table.As("oldTable"));
            Assert.IsType<DeltaTable>(table.Alias("oldTable"));
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

            // Generate should return void.
            table.Generate("symlink_format_manifest");

            // Delete should return void.
            table.Delete("id > 10");
            table.Delete(Functions.Expr("id > 5"));
            table.Delete();

            // Load the table as a streaming source.
            Assert.IsType<DataFrame>(_spark
                .ReadStream()
                .Format("delta")
                .Option("path", path)
                .Load());
            Assert.IsType<DataFrame>(_spark.ReadStream().Format("delta").Load(path));

            // Create Parquet data and convert it to DeltaTables.
            string parquetIdentifier = $"parquet.`{path}`";
            rangeRate.Write().Mode(SaveMode.Overwrite).Parquet(path);
            Assert.IsType<DeltaTable>(DeltaTable.ConvertToDelta(_spark, parquetIdentifier));
            rangeRate
                .Select(Functions.Col("id"), Functions.Expr($"(`id` + 1) AS `id_plus_one`"))
                .Write()
                .PartitionBy("id")
                .Mode(SaveMode.Overwrite)
                .Parquet(path);
            Assert.IsType<DeltaTable>(DeltaTable.ConvertToDelta(
                _spark,
                parquetIdentifier,
                "id bigint"));
            Assert.IsType<DeltaTable>(DeltaTable.ConvertToDelta(
                _spark,
                parquetIdentifier,
                new StructType(new[]
                {
                    new StructField("id", new IntegerType())
                })));
        }

        /// <summary>
        /// Test that Delta Lake 0.7+ methods return the expected signature.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_0_0)]
        public void TestSignaturesV3_0_X()
        {
            string tableName = "my_new_table";
            _spark.Range(15).Write().Format("delta").SaveAsTable(tableName);

            Assert.IsType<DeltaTable>(DeltaTable.ForName(tableName));
            DeltaTable table = DeltaTable.ForName(_spark, tableName);

            table.UpgradeTableProtocol(1, 3);
        }

        /// <summary>
        /// Validate that a range DataFrame contains only the expected values.
        /// </summary>
        /// <param name="expectedValues"></param>
        /// <param name="dataFrame"></param>
        private void ValidateRangeDataFrame(IEnumerable<int> expectedValues, DataFrame dataFrame)
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
