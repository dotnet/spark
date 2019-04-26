// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class DataFrameWriterTests
    {
        private readonly SparkSession _spark;

        public DataFrameWriterTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            {
                DataFrameWriter dfw = _spark
                    .Read()
                    .Schema("age INT, name STRING")
                    .Json(TestEnvironment.ResourceDirectory + "people.json")
                    .Write();

                Assert.IsType<DataFrameWriter>(dfw.Mode(SaveMode.Ignore));

                Assert.IsType<DataFrameWriter>(dfw.Mode("overwrite"));

                Assert.IsType<DataFrameWriter>(dfw.Format("json"));

                Assert.IsType<DataFrameWriter>(dfw.Option("stringOption", "value"));
                Assert.IsType<DataFrameWriter>(dfw.Option("boolOption", true));
                Assert.IsType<DataFrameWriter>(dfw.Option("longOption", 1L));
                Assert.IsType<DataFrameWriter>(dfw.Option("doubleOption", 3D));

                Assert.IsType<DataFrameWriter>(
                    dfw.Options(
                        new Dictionary<string, string>
                        {
                            { "option1", "value1" },
                            { "option2", "value2" }
                        }));


                Assert.IsType<DataFrameWriter>(dfw.PartitionBy("age"));
                Assert.IsType<DataFrameWriter>(dfw.PartitionBy("age", "name"));

                Assert.IsType<DataFrameWriter>(dfw.BucketBy(3, "age"));
                Assert.IsType<DataFrameWriter>(dfw.BucketBy(3, "age", "name"));

                Assert.IsType<DataFrameWriter>(dfw.SortBy("name"));
            }

            using (var tempDir = new TemporaryDirectory(TestEnvironment.ResourceDirectory))
            {
                DataFrameWriter dfw = _spark
                    .Read()
                    .Csv(TestEnvironment.ResourceDirectory + "people.csv")
                    .Write();

                // TODO: Test dfw.Jdbc without running a local db.

                dfw.SaveAsTable("TestTable");

                dfw.InsertInto("TestTable");

                dfw.Option("path", Path.Combine(tempDir.Path, "TestSavePath1")).Save();
                dfw.Save(Path.Combine(tempDir.Path, "TestSavePath2"));

                dfw.Json(Path.Combine(tempDir.Path, "TestJsonPath"));

                dfw.Parquet(Path.Combine(tempDir.Path, "TestParquetPath"));

                dfw.Orc(Path.Combine(tempDir.Path, "TestOrcPath"));

                dfw.Text(Path.Combine(tempDir.Path, "TestTextPath"));

                dfw.Csv(Path.Combine(tempDir.Path, "TestCsvPath"));
            }
        }
    }
}
