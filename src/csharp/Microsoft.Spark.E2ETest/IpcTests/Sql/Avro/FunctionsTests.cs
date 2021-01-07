// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Catalog;
using Microsoft.Spark.Sql.Types;
using Xunit;
using static Microsoft.Spark.Sql.Functions;
using static Microsoft.Spark.Sql.Avro.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests.Avro
{
    [Collection("Spark E2E Tests")]
    public class FunctionsTests
    {
        private readonly SparkSession _spark;

        public FunctionsTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for Avro APIs introduced in Spark 3.0.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_0_0)]
        public void TestSignaturesV3_0_X()
        {
            Column col = Column("col");
            string jsonSchema = "[{\"col\":0}]";
            var options = new Dictionary<string, string>() { { "key", "value" } };

            //Assert.IsType<Column>(FromAvro(col, jsonSchema));
            //Assert.IsType<Column>(FromAvro(col, jsonSchema, options));

            Assert.IsType<Column>(ToAvro(col));
            Assert.IsType<Column>(ToAvro(col, jsonSchema));
        }
    }
}
