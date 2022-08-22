// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

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

        public RowTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestWithDuplicatedRows()
        {
            var timestamp = new Timestamp(2020, 1, 1, 0, 0, 0, 0);
            var schema = new StructType(new StructField[]
            {
                new StructField("ts", new TimestampType())
            });
            var data = new GenericRow[]
            {
                new GenericRow(new object[] { timestamp })
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);
            Row[] rows = df
                .WithColumn("tsRow", Struct("ts"))
                .WithColumn("tsRowRow", Struct("tsRow"))
                .Collect()
                .ToArray();

            Assert.Single(rows);

            Row row = rows[0];
            Assert.Equal(3, row.Values.Length);
            Assert.Equal(timestamp, row.Values[0]);

            Row tsRow = row.Values[1] as Row;
            Assert.Single(tsRow.Values);
            Assert.Equal(timestamp, tsRow.Values[0]);

            Row tsRowRow = row.Values[2] as Row;
            Assert.Single(tsRowRow.Values);
            Assert.Equal(tsRowRow.Values[0], tsRow);
        }

        [Fact]
        public void TestWithDuplicateTimestamps()
        {
            var timestamp = new Timestamp(2020, 1, 1, 0, 0, 0, 0);
            var schema = new StructType(new StructField[]
            {
                new StructField("ts", new TimestampType())
            });
            var data = new GenericRow[]
            {
                new GenericRow(new object[] { timestamp }),
                new GenericRow(new object[] { timestamp }),
                new GenericRow(new object[] { timestamp })
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);
            Row[] rows = df.Collect().ToArray();

            Assert.Equal(3, rows.Length);
            foreach (Row row in rows)
            {
                Assert.Single(row.Values);
                Assert.Equal(timestamp, row.GetAs<Timestamp>(0));
            }
        }

        [Fact]
        public void TestWithDuplicateDates()
        {
            var date = new Date(2020, 1, 1);
            var schema = new StructType(new StructField[]
            {
                new StructField("date", new DateType())
            });
            var data = new GenericRow[]
            {
                new GenericRow(new object[] { date }),
                new GenericRow(new object[] { date }),
                new GenericRow(new object[] { date })
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);

            Row[] rows = df.Collect().ToArray();

            Assert.Equal(3, rows.Length);
            foreach (Row row in rows)
            {
                Assert.Single(row.Values);
                Assert.Equal(date, row.GetAs<Date>(0));
            }
        }
    }
}
