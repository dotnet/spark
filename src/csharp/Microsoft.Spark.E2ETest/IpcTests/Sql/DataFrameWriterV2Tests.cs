// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
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

            DataFrameWriterV2 dfwV2 = df.WriteTo("testTable");

            //Assert.IsAssignableFrom<CreateTableWriter>(dfwV2.Using("json"));

            Assert.IsType<DataFrameWriterV2>(dfwV2.Option("key", "value"));

            Assert.IsType<DataFrameWriterV2>(dfwV2.Options(
                new Dictionary<string, string>() { { "key", "value" } }));

            //Assert.IsAssignableFrom<CreateTableWriter>(dfwV2.TableProperty("prop", "value"));

            //Assert.IsAssignableFrom<CreateTableWriter>(dfwV2.PartitionedBy(df.Col("age")));

            dfwV2.Create();

            //dfwV2.Replace();

            //dfwV2.CreateOrReplace();

            //dfwV2.Append();

            //dfwV2.Overwrite(df.Col("age"));

            //dfwV2.OverwritePartitions();
        }
    }
}
