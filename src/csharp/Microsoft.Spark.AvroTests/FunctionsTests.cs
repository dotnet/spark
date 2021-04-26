// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Xunit;
using static Microsoft.Spark.Sql.Avro.Functions;

namespace Microsoft.Spark.AvroTests
{
    [Collection(Constants.AvroTestContainerName)]
    public class FunctionsTests
    {
        private readonly SparkSession _spark;

        public FunctionsTests(AvroFixture fixture)
        {
            _spark = fixture.SparkFixture.Spark;
        }

        /// <summary>
        /// Test signatures for Avro APIs introduced in Spark 3.0.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_0_0)]
        public void TestSignaturesV3_0_X()
        {
            DataFrame df = _spark.Range(1);
            string jsonSchema = "{\"type\":\"long\", \"name\":\"col\"}";
            var options = new Dictionary<string, string>() { { "mode", "PERMISSIVE" } };

            Column inputCol = df.Col("id");
            Column avroCol = ToAvro(inputCol);

            Assert.IsType<Column>(avroCol);
            Assert.IsType<Column>(ToAvro(inputCol, jsonSchema));
            Assert.IsType<Column>(FromAvro(avroCol, jsonSchema));
            Assert.IsType<Column>(FromAvro(avroCol, jsonSchema, options));
        }
    }
}
