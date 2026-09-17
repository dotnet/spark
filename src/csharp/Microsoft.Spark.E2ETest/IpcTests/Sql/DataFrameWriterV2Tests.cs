// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    [Trait("Category", "DriverApi")]
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

            // The JSON provider creates a V1 table, which must reject these V2 operations.
            AssertUnsupportedTableOperation(() => dfwV2.Replace());
            AssertUnsupportedTableOperation(() => dfwV2.CreateOrReplace());
            AssertUnsupportedTableOperation(() => dfwV2.Append());
            // Use an unbound false predicate to reach the overwrite-by-filter capability check.
            AssertUnsupportedTableOperation(() => dfwV2.Overwrite(Functions.Lit(false)));
            AssertUnsupportedTableOperation(() => dfwV2.OverwritePartitions());
        }

        private static void AssertUnsupportedTableOperation(Action operation)
        {
            Exception exception = Assert.Throws<Exception>(operation);
            JvmException jvmException = Assert.IsType<JvmException>(exception.InnerException);
            Assert.Contains("org.apache.spark.sql.AnalysisException", jvmException.Message);

            string message = jvmException.Message.ToLowerInvariant();
            Assert.True(
                message.Contains("only supported") ||
                message.Contains("does not support") ||
                message.Contains("unsupported") ||
                message.Contains("cannot write into v1 table"),
                jvmException.Message);
        }
    }
}
