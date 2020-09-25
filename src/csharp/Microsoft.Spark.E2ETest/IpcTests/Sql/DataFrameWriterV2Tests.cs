// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class DataFrameWriterV2Tests
    {
        private readonly SparkSession _spark;

        public DataFrameWriterV2Tests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_0_0)]
        public void TestSignaturesV3_0_X()
        {
            DataFrame df = _spark
                .Read()
                .Schema("age INT, name STRING")
                .Json($"{TestEnvironment.ResourceDirectory}people.json");

            DataFrameWriterV2 dfwV2 = df.WriteTo("testtable");

            Assert.IsType<DataFrameWriterV2>(dfwV2.Using("json"));

            Assert.IsType<DataFrameWriterV2>(dfwV2.Option("key1", "value"));
            Assert.IsType<DataFrameWriterV2>(dfwV2.Option("key2", true));
            Assert.IsType<DataFrameWriterV2>(dfwV2.Option("key3", 1L));
            Assert.IsType<DataFrameWriterV2>(dfwV2.Option("key4", 2D));

            Assert.IsType<DataFrameWriterV2>(dfwV2.Options(
                new Dictionary<string, string>() { { "key", "value" } }));

            Assert.IsType<DataFrameWriterV2>(dfwV2.TableProperty("prop", "value"));

            _spark.Sql("DROP TABLE IF EXISTS default.testtable");
            dfwV2.Create();

            Assert.IsType<DataFrameWriterV2>(dfwV2.PartitionedBy(df.Col("age")));


            // Testing caveat *************************************************************
            // The following functions cannot be tested because of lack of support for DataSourceV2.
            // This is because Spark 3.0 currently doesn't support file source as provider for
            // tables. Issue - https://issues.apache.org/jira/browse/SPARK-28396
            //
            // 1. DataFrameWriterV2.Replace()
            // 2. DataFrameWriterV2.CreateOrReplace()
            // 3. DataFrameWriterV2.Append()
            // 4. DataFrameWriterV2.Overwrite()
            // 5. DataFrameWriterV2.OverwritePartitions()
            // *****************************************************************************
        }
    }
}
