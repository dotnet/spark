// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;
using Xunit;
using static Microsoft.Spark.Sql.Expressions.Window;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class WindowSpecTests
    {
        /// <summary>
        /// Test signatures for APIs up to Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            Column col1 = Column("age");
            Column col2 = Column("name");
            WindowSpec windowSpec = PartitionBy("age");

            Assert.IsType<WindowSpec>(windowSpec.PartitionBy("age"));
            Assert.IsType<WindowSpec>(windowSpec.PartitionBy("age", "name"));
            Assert.IsType<WindowSpec>(windowSpec.PartitionBy());
            Assert.IsType<WindowSpec>(windowSpec.PartitionBy(col1));
            Assert.IsType<WindowSpec>(windowSpec.PartitionBy(col1, col2));

            Assert.IsType<WindowSpec>(windowSpec.OrderBy("age"));
            Assert.IsType<WindowSpec>(windowSpec.OrderBy("age", "name"));
            Assert.IsType<WindowSpec>(windowSpec.OrderBy());
            Assert.IsType<WindowSpec>(windowSpec.OrderBy(col1));
            Assert.IsType<WindowSpec>(windowSpec.OrderBy(col1, col2));

            Assert.IsType<WindowSpec>(
                windowSpec.RowsBetween(
                    Sql.Expressions.Window.UnboundedPreceding,
                    Sql.Expressions.Window.UnboundedFollowing));

            Assert.IsType<WindowSpec>(
                windowSpec.RangeBetween(
                    Sql.Expressions.Window.UnboundedPreceding,
                    Sql.Expressions.Window.UnboundedFollowing));

            if (SparkSettings.Version < new Version(Versions.V3_0_0))
            {
                // The following APIs are removed in Spark 3.0.
                Assert.IsType<WindowSpec>(
                    windowSpec.RangeBetween(
                        UnboundedPreceding(),
                        UnboundedFollowing()));
            }
        }
    }
}
