// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using static Microsoft.Spark.Sql.Functions;


namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class RowTests
    {
        private readonly SparkSession _spark;
        private readonly DataFrame _df;

        public RowTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            _df = _spark
                .Read()
                .Schema("age INT, name STRING")
                .Json($"{TestEnvironment.ResourceDirectory}people.json");
        }

        [Fact]
        public void TestCollect()
        {
            Row[] rows = _df.Collect().ToArray();
            Assert.Equal(3, rows.Length);

            Row row1 = rows[0];
            Assert.Equal("Michael", row1.GetAs<string>("name"));
            Assert.Null(row1.Get("age"));

            Row row2 = rows[1];
            Assert.Equal("Andy", row2.GetAs<string>("name"));
            Assert.Equal(30, row2.GetAs<int>("age"));

            Row row3 = rows[2];
            Assert.Equal("Justin", row3.GetAs<string>("name"));
            Assert.Equal(19, row3.GetAs<int>("age"));
        }

        [Fact]
        public void TestWithColumn()
        {
            Func<Column, Column> AgeIsNull = Udf<Row, bool>(
                r =>
                {
                    var age = r.GetAs<int?>("age");
                    return !age.HasValue;
                });

            string[] allCols = _df.Columns().ToArray();
            DataFrame dummyColDF =
                _df.WithColumn("DummyCol", Struct(allCols[0], allCols.Skip(1).ToArray()));
            DataFrame ageIsNullDF =
                dummyColDF.WithColumn("AgeIsNull", AgeIsNull(dummyColDF["DummyCol"]));

            Row[] originalRows = _df.Collect().ToArray();
            Row[] rows = ageIsNullDF.Collect().ToArray();
            Assert.Equal(3, rows.Length);

            {
                Row row = rows[0];
                Assert.Equal("Michael", row.GetAs<string>("name"));
                Assert.Null(row.Get("age"));
                Assert.Equal(originalRows[0], row.Get("DummyCol"));
                Assert.True(row.GetAs<bool>("AgeIsNull"));
            }

            {
                Row row = rows[1];
                Assert.Equal("Andy", row.GetAs<string>("name"));
                Assert.Equal(30, row.GetAs<int>("age"));
                Assert.Equal(originalRows[1], row.Get("DummyCol"));
                Assert.False(row.GetAs<bool>("AgeIsNull"));
            }

            {
                Row row = rows[2];
                Assert.Equal("Justin", row.GetAs<string>("name"));
                Assert.Equal(19, row.GetAs<int>("age"));
                Assert.Equal(originalRows[2], row.Get("DummyCol"));
                Assert.False(row.GetAs<bool>("AgeIsNull"));
            }
        }
    }
}
