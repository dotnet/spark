// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

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
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            Column col1 = Column("age");
            Column col2 = Column("name");

            _ = Sql.Expressions.Window.UnboundedPreceding;
            _ = Sql.Expressions.Window.UnboundedFollowing;
            _ = Sql.Expressions.Window.CurrentRow;

            WindowSpec windowSpec = PartitionBy("age");
            windowSpec = PartitionBy("age", "name");
            windowSpec = PartitionBy();
            windowSpec = PartitionBy(col1);
            windowSpec = PartitionBy(col1, col2);

            windowSpec = OrderBy("age");
            windowSpec = OrderBy("age", "name");
            windowSpec = OrderBy();
            windowSpec = OrderBy(col1);
            windowSpec = OrderBy(col1, col2);

            windowSpec = RowsBetween(
                Sql.Expressions.Window.UnboundedPreceding,
                Sql.Expressions.Window.UnboundedFollowing);

            windowSpec = RangeBetween(
                Sql.Expressions.Window.UnboundedPreceding,
                Sql.Expressions.Window.UnboundedFollowing);
            windowSpec = RangeBetween(UnboundedPreceding(), UnboundedFollowing());
        }
    }
}
