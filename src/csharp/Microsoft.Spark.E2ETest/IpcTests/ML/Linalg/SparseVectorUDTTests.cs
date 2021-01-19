// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.ML.Linalg;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Linalg
{
    [Collection("Spark E2E Tests")]
    public class SparseVectorUDTTests
    {
        private readonly SparkSession _spark;

        public SparseVectorUDTTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test that we can create a SparseVectorUDT
        /// </summary>
        [Fact]
        public void TestPassingBetweenApacheSpark()
        {
            var type = new SparseVectorUDT();
            Assert.IsType<JObject>(type.JsonValue);
            Assert.IsType<string>(type.Json);
            Assert.IsType<string>(type.SimpleString);
            Assert.IsType<string>(type.TypeName);
            Assert.True(type.NeedConversion());
        }
    }
}
