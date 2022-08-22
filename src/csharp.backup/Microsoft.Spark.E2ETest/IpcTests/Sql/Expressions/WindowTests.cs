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
    public class WindowTests
    {
        /// <summary>
        /// Test signatures for APIs up to Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            Column col1 = Column("age");
            Column col2 = Column("name");

            Assert.IsType<long>(Sql.Expressions.Window.UnboundedPreceding);
            Assert.IsType<long>(Sql.Expressions.Window.UnboundedFollowing);
            Assert.IsType<long>(Sql.Expressions.Window.CurrentRow);

            Assert.IsType<WindowSpec>(PartitionBy("age"));
            Assert.IsType<WindowSpec>(PartitionBy("age", "name"));
            Assert.IsType<WindowSpec>(PartitionBy());
            Assert.IsType<WindowSpec>(PartitionBy(col1));
            Assert.IsType<WindowSpec>(PartitionBy(col1, col2));

            Assert.IsType<WindowSpec>(OrderBy("age"));
            Assert.IsType<WindowSpec>(OrderBy("age", "name"));
            Assert.IsType<WindowSpec>(OrderBy());
            Assert.IsType<WindowSpec>(OrderBy(col1));
            Assert.IsType<WindowSpec>(OrderBy(col1, col2));

            Assert.IsType<WindowSpec>(
                RowsBetween(
                    Sql.Expressions.Window.UnboundedPreceding,
                    Sql.Expressions.Window.UnboundedFollowing));

            Assert.IsType<WindowSpec>(
                RangeBetween(
                    Sql.Expressions.Window.UnboundedPreceding,
                    Sql.Expressions.Window.UnboundedFollowing));

            if (SparkSettings.Version < new Version(Versions.V3_0_0))
            {
                // The following APIs are removed in Spark 3.0.
                Assert.IsType<WindowSpec>(
                    RangeBetween(
                        UnboundedPreceding(),
                        UnboundedFollowing()));
            }
        }
    }
}
