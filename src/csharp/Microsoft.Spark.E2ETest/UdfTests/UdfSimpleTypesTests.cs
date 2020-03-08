// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.UdfTests
{
    [Collection("Spark E2E Tests")]
    public class UdfSimpleTypesTests
    {
        private readonly SparkSession _spark;
        private readonly DataFrame _df;

        public UdfSimpleTypesTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            var data = new List<GenericRow>();
            data.Add(new GenericRow(
                new object[]
                {
                    null,
                    new Date(2020, 1, 1),
                    new Timestamp(2020, 1, 1, 0, 0, 0, 0)
                }));
            data.Add(new GenericRow(
                new object[]
                {
                    30,
                    new Date(2020, 1, 2),
                    new Timestamp(2020, 1, 2, 15, 30, 30, 0)
                }));
            var schema = new StructType(new List<StructField>()
                {
                    new StructField("age", new IntegerType()),
                    new StructField("date", new DateType()),
                    new StructField("time", new TimestampType())
                });
            _df = _spark.CreateDataFrame(data, schema);
        }

        /// <summary>
        /// UDF that takes in Date type.
        /// </summary>
        [Fact]
        public void TestUdfWithDateType()
        {
            Func<Column, Column> udf = Udf<Date, string>(date => date.ToString());

            Row[] rows = _df.Select(udf(_df["date"])).Collect().ToArray();
            Assert.Equal(2, rows.Length);

            var expected = new string[] { "2020-01-01", "2020-01-02" };
            string[] rowsToArray = rows.Select(x => x[0].ToString()).ToArray();
            Assert.Equal(expected, rowsToArray);
        }

        /// <summary>
        /// UDF that returns Date type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsDateType()
        {
            Func<Column, Column> udf1 = Udf<int?, Date>(
                s => new Date(2020 + s.GetValueOrDefault(), 1, 4));
            Func<Column, Column> udf2 = Udf<Date, string>(date => date.ToString());

            // Test UDF that returns a Date object.
            {
                Row[] rows = _df.Select(udf1(_df["age"]).Alias("col")).Collect().ToArray();
                Assert.Equal(2, rows.Length);

                var expected = new Date[]
                {
                    new Date(2020, 1, 4),
                    new Date(2050, 1, 4),
                };
                for (int i = 0; i < rows.Length; ++i)
                {
                    Assert.Equal(1, rows[i].Size());
                    Assert.Equal(expected[i], rows[i].GetAs<Date>("col"));
                }
            }

            // Chained UDFs.
            {
                Row[] rows = _df.Select(udf2(udf1(_df["age"]))).Collect().ToArray();
                Assert.Equal(2, rows.Length);

                var expected = new string[] { "2020-01-04", "2050-01-04" };
                for (int i = 0; i < rows.Length; ++i)
                {
                    Assert.Equal(1, rows[i].Size());
                    Assert.Equal(expected[i], rows[i].GetAs<string>(0));
                }
            }
        }

        /// <summary>
        /// UDF that takes in Timestamp type.
        /// </summary>
        [Fact]
        public void TestUdfWithTimestampType()
        {
            Func<Column, Column> udf = Udf<Timestamp, string>(time => time.ToString());

            Row[] rows = _df.Select(udf(_df["time"])).Collect().ToArray();
            Assert.Equal(2, rows.Length);

            var expected = new string[]
            {
                "2020-01-01 00:00:00.000000",
                "2020-01-02 15:30:30.000000"
            };
            string[] rowsToArray = rows.Select(x => x[0].ToString()).ToArray();
            Assert.Equal(expected, rowsToArray);
        }

        /// <summary>
        /// UDF that returns Timestamp type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsTimestampType()
        {
            Func<Column, Column> udf1 = Udf<int?, Timestamp>(
                s => new Timestamp(2020 + s.GetValueOrDefault(), 1, 4, 0, 0, 0, 0));
            Func<Column, Column> udf2 = Udf<Timestamp, string>(time => time.ToString());

            // Test UDF that returns a Timestamp object.
            {
                Row[] rows = _df.Select(udf1(_df["age"]).Alias("col")).Collect().ToArray();
                Assert.Equal(2, rows.Length);

                var expected = new Timestamp[]
                {
                    new Timestamp(2020, 1, 4, 0, 0, 0, 0),
                    new Timestamp(2050, 1, 4, 0, 0, 0, 0),
                };
                for (int i = 0; i < rows.Length; ++i)
                {
                    Assert.Equal(1, rows[i].Size());
                    Assert.Equal(expected[i], rows[i].GetAs<Timestamp>("col"));
                }
            }

            // Chained UDFs.
            {
                Row[] rows = _df.Select(udf2(udf1(_df["age"]))).Collect().ToArray();
                Assert.Equal(2, rows.Length);

                var expected = new string[]
                {
                    "2020-01-04 00:00:00.000000",
                    "2050-01-04 00:00:00.000000"
                };
                for (int i = 0; i < rows.Length; ++i)
                {
                    Assert.Equal(1, rows[i].Size());
                    Assert.Equal(expected[i], rows[i].GetAs<string>(0));
                }
            }
        }
    }
}
