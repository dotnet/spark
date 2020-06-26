// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class JvmBridgeTests
    {
        private readonly SparkSession _spark;

        public JvmBridgeTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestInnerJvmException()
        {
            try
            {
                _spark.Sql("THROW!!!");
            }
            catch (Exception ex)
            {
                Assert.NotNull(ex.InnerException);
                Assert.IsType<JvmException>(ex.InnerException);
                Assert.False(string.IsNullOrWhiteSpace(ex.InnerException.Message));
            }
        }
    }
}
