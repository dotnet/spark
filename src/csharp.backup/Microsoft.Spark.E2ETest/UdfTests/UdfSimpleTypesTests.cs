// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
            var data = new List<GenericRow>
            {
                new GenericRow(
                    new object[]
                    {
                        null,
                        new Date(2020, 1, 1),
                        new Timestamp(2020, 1, 1, 0, 0, 0, 0)
                    }),
                new GenericRow(
                    new object[]
                    {
                        30,
                        new Date(2020, 1, 2),
                        new Timestamp(2020, 1, 2, 15, 30, 30, 123456)
                    })
            };
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
                i => new Date(2020 + i.GetValueOrDefault(), 1, 4));
            Func<Column, Column> udf2 = Udf<Date, string>(date => date.ToString());

            // Test UDF that returns a Date object.
            {
                Row[] rows = _df.Select(udf1(_df["age"]).Alias("col")).Collect().ToArray();
                Assert.Equal(2, rows.Length);

                var expected = new Date[]
                {
                    new Date(2020, 1, 4),
                    new Date(2050, 1, 4)
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
                "2020-01-01 00:00:00.000000Z",
                "2020-01-02 15:30:30.123456Z"
            };
            string[] rowsToArray = rows.Select(x => x[0].ToString()).ToArray();
            Assert.Equal(expected, rowsToArray);
        }

        /// <summary>
        /// UDF that returns a timestamp string.
        /// </summary>
        [Fact]
        public void TestUdfWithDuplicateTimestamps()
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

            var expectedTimestamp = new Timestamp(1970, 1, 2, 0, 0, 0, 0);
            Func<Column, Column> udf = Udf<Timestamp, Timestamp>(
                ts => new Timestamp(1970, 1, 2, 0, 0, 0, 0));

            DataFrame df = _spark.CreateDataFrame(data, schema);

            Row[] rows = df.Select(udf(df["ts"])).Collect().ToArray();

            Assert.Equal(3, rows.Length);
            foreach (Row row in rows)
            {
                Assert.Single(row.Values);
                Assert.Equal(expectedTimestamp, row.Values[0]);
            }
        }

        /// <summary>
        /// UDF that returns Timestamp type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsTimestampType()
        {
            Func<Column, Column> udf1 = Udf<int?, Timestamp>(
                i => new Timestamp(2020 + i.GetValueOrDefault(), 1, 4, 15, 30, 30, 123456));
            Func<Column, Column> udf2 = Udf<Timestamp, string>(time => time.ToString());

            // Test UDF that returns a Timestamp object.
            {
                Row[] rows = _df.Select(udf1(_df["age"]).Alias("col")).Collect().ToArray();
                Assert.Equal(2, rows.Length);

                var expected = new Timestamp[]
                {
                    new Timestamp(2020, 1, 4, 15, 30, 30, 123456),
                    new Timestamp(2050, 1, 4, 15, 30, 30, 123456),
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
                    "2020-01-04 15:30:30.123456Z",
                    "2050-01-04 15:30:30.123456Z"
                };
                for (int i = 0; i < rows.Length; ++i)
                {
                    Assert.Equal(1, rows[i].Size());
                    Assert.Equal(expected[i], rows[i].GetAs<string>(0));
                }
            }
        }

        /// <summary>
        /// Test to validate UDFs defined in separate threads work.
        /// </summary>
        [Fact]
        public void TestUdfWithMultipleThreads()
        {
            try
            {
                static void DefineUdf() => Udf<string, string>(str => str);

                // Define a UDF in the main thread.
                Udf<string, string>(str => str);

                // Verify a UDF can be defined in a separate thread.
                Thread t = new Thread(DefineUdf);
                t.Start();
                t.Join();
            }
            catch (Exception)
            {
                Assert.True(false);
            }
        }
    }
}
