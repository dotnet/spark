// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.E2ETest.Utils;
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
        /// Test signatures for APIs up to Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            Column col1 = Column("col1");
            Column col2 = Column("col2");

            Assert.IsType<Column>(-col1);

            Assert.IsType<Column>(!col1);

            Assert.IsType<Column>(col1 == col2);
            Assert.IsType<Column>(col1.EqualTo(col2));

            Assert.IsType<Column>(col1 != col2);
            Assert.IsType<Column>(col1.NotEqual(col2));

            Assert.IsType<Column>(col1 > col2);
            Assert.IsType<Column>(col1 > "hello");
            Assert.IsType<Column>(col1.Gt(col2));
            Assert.IsType<Column>(col1.Gt("hello"));

            Assert.IsType<Column>(col1 < col2);
            Assert.IsType<Column>(col1 < "hello");
            Assert.IsType<Column>(col1.Lt(col2));
            Assert.IsType<Column>(col1.Lt("hello"));

            Assert.IsType<Column>(col1 <= col2);
            Assert.IsType<Column>(col1 <= "hello");
            Assert.IsType<Column>(col1.Leq(col2));
            Assert.IsType<Column>(col1.Leq("hello"));

            Assert.IsType<Column>(col1 >= col2);
            Assert.IsType<Column>(col1 >= "hello");
            Assert.IsType<Column>(col1.Geq(col2));
            Assert.IsType<Column>(col1.Geq("hello"));

            Assert.IsType<Column>(col1.EqNullSafe(col2));
            Assert.IsType<Column>(col1.EqNullSafe("hello"));

            Assert.IsType<Column>(When(col1 == col2, 0).When(col1 == col2, 0));

            Assert.IsType<Column>(When(col1 == col2, 0).Otherwise(col2));
            Assert.IsType<Column>(When(col1 == col2, 0).Otherwise("hello"));

            Assert.IsType<Column>(col1.Between(col1, col2));
            Assert.IsType<Column>(col1.Between(1, 3));

            Assert.IsType<Column>(col1.IsNaN());

            Assert.IsType<Column>(col1.IsNotNull());

            Assert.IsType<Column>(col1 | col2);
            Assert.IsType<Column>(col1.Or(col2));

            Assert.IsType<Column>(col1 & col2);
            Assert.IsType<Column>(col1.And(col2));

            Assert.IsType<Column>(col1 + col2);
            Assert.IsType<Column>(col1.Plus(col2));

            Assert.IsType<Column>(col1 - col2);
            Assert.IsType<Column>(col1.Minus(col2));

            Assert.IsType<Column>(col1 * col2);
            Assert.IsType<Column>(col1.Multiply(col2));

            Assert.IsType<Column>(col1 / col2);
            Assert.IsType<Column>(col1.Divide(col2));

            Assert.IsType<Column>(col1 % col2);
            Assert.IsType<Column>(col1.Mod(col2));

            Assert.IsType<Column>(col1.Like("hello"));

            Assert.IsType<Column>(col1.RLike("hello"));

            Assert.IsType<Column>(col1.GetItem(1));
            Assert.IsType<Column>(col1.GetItem("key"));

            Assert.IsType<Column>(col1.GetField("field"));

            Assert.IsType<Column>(col1.SubStr(col1, col2));
            Assert.IsType<Column>(col1.SubStr(0, 5));

            Assert.IsType<Column>(col1.Contains(col2));
            Assert.IsType<Column>(col1.Contains("hello"));

            Assert.IsType<Column>(col1.StartsWith(col2));
            Assert.IsType<Column>(col1.StartsWith("hello"));

            Assert.IsType<Column>(col1.EndsWith(col2));
            Assert.IsType<Column>(col1.EndsWith("hello"));

            Assert.IsType<Column>(col1.Alias("alias"));

            Assert.IsType<Column>(col1.As("alias"));
            Assert.IsType<Column>(col1.As(new string[] { }));
            Assert.IsType<Column>(col1.As(new[] { "alias1", "alias2" }));

            Assert.IsType<Column>(col1.Apply(col2));

            Assert.IsType<Column>(col1.Name("alias"));

            Assert.IsType<Column>(col1.Cast("string"));

            Assert.IsType<Column>(col1.Desc());
            Assert.IsType<Column>(col1.DescNullsFirst());
            Assert.IsType<Column>(col1.DescNullsLast());

            Assert.IsType<Column>(col1.Asc());
            Assert.IsType<Column>(col1.AscNullsFirst());
            Assert.IsType<Column>(col1.AscNullsLast());

            col1.Explain(true);

            Assert.IsType<Column>(col1.BitwiseOR(col2));

            Assert.IsType<Column>(col1.BitwiseAND(col2));

            Assert.IsType<Column>(col1.BitwiseXOR(col2));

            Assert.IsType<Column>(col1.Over(PartitionBy(col1)));
            Assert.IsType<Column>(col1.Over());

            Assert.Equal("col1", col1.ToString());
            Assert.Equal("col2", col2.ToString());
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.1.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_1_0)]
        public void TestSignaturesV3_1_X()
        {
            Column col = Column("col");

            Assert.IsType<Column>(col.WithField("col2", Lit(3)));

            Assert.IsType<Column>(col.DropFields("col"));
            Assert.IsType<Column>(col.DropFields("col", "col2"));
        }
    }
}
