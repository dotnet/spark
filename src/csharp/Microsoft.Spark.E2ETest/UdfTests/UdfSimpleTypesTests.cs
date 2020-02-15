// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
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
            _df = _spark
                .Read()
                .Schema("name STRING, date DATE")
                .Json(Path.Combine($"{TestEnvironment.ResourceDirectory}people.json"));
        }

        /// <summary>
        /// UDF that takes in Date type.
        /// </summary>
        [Fact]
        public void TestUdfWithDateType()
        {
            Func<Column, Column> udf = Udf<Date, string>(date => date.ToString());

            Row[] rows = _df.Select(udf(_df["date"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);

            var expected = new string[] { "1/1/2020", "1/2/2020", "1/3/2020" };
            string[] rowsToArray = rows.Select(x => x[0].ToString()).ToArray();
            Assert.Equal(expected, rowsToArray);
        }

        /// <summary>
        /// UDF that returns Date type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsDateType()
        {
            Func<Column, Column> udf1 = Udf<string, Date>(str => new Date(2020, 1, 4));
            Func<Column, Column> udf2 = Udf<Date, string>(date => date.ToString());

            // Test UDF that returns a Date object.
            {
                Row[] rows = _df.Select(udf1(_df["name"]).Alias("col")).Collect().ToArray();
                Assert.Equal(3, rows.Length);

                foreach (Row row in rows)
                {
                    Assert.Equal(1, row.Size());
                    Assert.Equal(new Date(2020, 1, 4), row.GetAs<Date>("col"));
                }
            }

            // Chained UDFs.
            {
                Row[] rows = _df.Select(udf2(udf1(_df["name"]))).Collect().ToArray();
                Assert.Equal(3, rows.Length);

                foreach (Row row in rows)
                {
                    Assert.Equal(1, row.Size());
                    Assert.Equal("1/4/2020", row.GetAs<string>(0));
                }
            }
        }       
    }
}
