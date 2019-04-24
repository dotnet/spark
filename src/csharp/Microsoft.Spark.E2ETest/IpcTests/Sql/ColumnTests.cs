// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql;
using Xunit;
using static Microsoft.Spark.Sql.Expressions.Window;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class ColumnTests
    {
        /// <summary>
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            Column col = Column("col");
            Column col1 = Column("col1");
            Column col2 = Column("col2");

            col = -col1;

            col = !col;

            col = col1 == col2;
            col = col1.EqualTo(col2);

            col = col1 != col2;
            col = col1.NotEqual(col2);

            col = col1 > col2;
            col = col1 > "hello";
            col = col1.Gt(col2);
            col = col1.Gt("hello");

            col = col1 < col2;
            col = col1 < "hello";
            col = col1.Lt(col2);
            col = col1.Lt("hello");

            col = col1 <= col2;
            col = col1 <= "hello";
            col = col1.Leq(col2);
            col = col1.Leq("hello");

            col = col1 >= col2;
            col = col1 >= "hello";
            col = col1.Geq(col2);
            col = col1.Geq("hello");

            col = col1.EqNullSafe(col2);
            col = col1.EqNullSafe("hello");

            col = When(col1 == col2, 0).When(col1 == col2, 0);

            col = When(col1 == col2, 0).Otherwise(col2);
            col = When(col1 == col2, 0).Otherwise("hello");

            col = col1.Between(col1, col2);
            col = col1.Between(1, 3);

            col = col.IsNaN();

            col = col.IsNotNull();

            col = col1 | col2;
            col = col.Or(col2);

            col = col1 & col2;
            col = col.And(col2);

            col = col1 + col2;
            col = col1.Plus(col2);

            col = col1 - col2;
            col = col1.Minus(col2);

            col = col1 * col2;
            col = col1.Multiply(col2);

            col = col1 / col2;
            col = col1.Divide(col2);

            col = col1 % col2;
            col = col1.Mod(col2);

            col = col1.Like("hello");

            col = col1.RLike("hello");

            col = col1.GetItem(1);
            col = col1.GetItem("key");

            col = col1.GetField("field");

            col = col1.SubStr(col1, col2);
            col = col1.SubStr(0, 5);

            col = col1.Contains(col2);
            col = col1.Contains("hello");

            col = col1.StartsWith(col2);
            col = col1.StartsWith("hello");

            col = col1.EndsWith(col2);
            col = col1.EndsWith("hello");

            col = col1.Alias("alias");

            col = col1.As("alias");
            col = col1.As(new string[] { });
            col = col1.As(new[] { "alias1", "alias2" });

            col = col1.Name("alias");

            col = col1.Cast("string");

            col = col1.Desc();
            col = col1.DescNullsFirst();
            col = col1.DescNullsLast();

            col = col1.Asc();
            col = col1.AscNullsFirst();
            col = col1.AscNullsLast();

            col.Explain(true);

            col = col1.BitwiseOR(col2);

            col = col1.BitwiseAND(col2);

            col = col1.BitwiseXOR(col2);

            col = col1.Over(PartitionBy(col1));
            col = col1.Over();
        }
    }
}
