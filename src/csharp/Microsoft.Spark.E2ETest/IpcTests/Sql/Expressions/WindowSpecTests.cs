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
    public class WindowSpecTests
    {
        /// <summary>
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            Column col1 = Column("age");
            Column col2 = Column("name");
            WindowSpec windowSpec = PartitionBy("age");

            windowSpec = windowSpec.PartitionBy("age");
            windowSpec = windowSpec.PartitionBy("age", "name");
            windowSpec = windowSpec.PartitionBy();
            windowSpec = windowSpec.PartitionBy(col1);
            windowSpec = windowSpec.PartitionBy(col1, col2);

            windowSpec = windowSpec.OrderBy("age");
            windowSpec = windowSpec.OrderBy("age", "name");
            windowSpec = windowSpec.OrderBy();
            windowSpec = windowSpec.OrderBy(col1);
            windowSpec = windowSpec.OrderBy(col1, col2);

            windowSpec = windowSpec.RowsBetween(
                Sql.Expressions.Window.UnboundedPreceding,
                Sql.Expressions.Window.UnboundedFollowing);

            windowSpec = windowSpec.RangeBetween(
                Sql.Expressions.Window.UnboundedPreceding,
                Sql.Expressions.Window.UnboundedFollowing);
            windowSpec = windowSpec.RangeBetween(UnboundedPreceding(), UnboundedFollowing());
        }
    }
}
