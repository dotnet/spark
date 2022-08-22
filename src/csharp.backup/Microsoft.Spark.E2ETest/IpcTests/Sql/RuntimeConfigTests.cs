// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class RuntimeConfigTests
    {
        private readonly SparkSession _spark;

        public RuntimeConfigTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.4.*.
        /// The purpose of this test is to ensure that JVM calls can be successfully made.
        /// Note that this is not testing functionality of each function.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            RuntimeConfig conf = _spark.Conf();

            conf.Set("stringKey", "stringValue");
            conf.Set("boolKey", false);
            conf.Set("longKey", 1234L);

            Assert.Equal("stringValue", conf.Get("stringKey"));
            Assert.Equal("false", conf.Get("boolKey"));
            Assert.Equal("1234", conf.Get("longKey"));

            conf.Unset("stringKey");
            Assert.Equal("defaultValue", conf.Get("stringKey", "defaultValue"));
            Assert.Equal("false", conf.Get("boolKey", "true"));

            Assert.True(conf.IsModifiable("spark.sql.streaming.checkpointLocation"));
            Assert.False(conf.IsModifiable("missingKey"));
        }
    }
}
