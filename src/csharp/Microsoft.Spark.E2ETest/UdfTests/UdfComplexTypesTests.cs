// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.Sql;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.UdfTests
{
    [Collection("Spark E2E Tests")]
    public class UdfComplexTypesTests
    {
        private readonly SparkSession _spark;
        private readonly DataFrame _df;

        public UdfComplexTypesTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            _df = _spark
                .Read()
                .Json(Path.Combine($"{TestEnvironment.ResourceDirectory}people_types.json"));
        }

        /// <summary>
        /// UDF that takes in Array type.
        /// </summary>
        [Fact]
        public void TestUdfWithArrayType()
        {
            // ArrayList works for this type.
            Func<Column, Column> udfInt = Udf<int[], string>(array => string.Join(',', array));

            Assert.Throws<Exception>(() => _df.Select(udfInt(_df["ages"])).Collect().ToArray());

            Func<Column, Column> udfString = Udf<string[], string>(
                array => string.Join(',', array));

            Assert.Throws<Exception>(() => _df.Select(udfString(_df["cars"])).Collect().ToArray());
        }

        /// <summary>
        /// UDF that returns Array type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnTypeAsArray()
        {
            Func<Column, Column> udf = Udf<string, string[]>(
                str => new string[] { str, str + str });

            Assert.Throws<NotImplementedException>(
                () => _df.Select(udf(_df["name"])).Collect().ToArray());
        }

        /// <summary>
        /// UDF that takes in Map type.
        /// </summary>
        [Fact]
        public void TestUdfWithMapType()
        {
            Func<Column, Column> udf = Udf<IDictionary<string, string>, string>(
                    dict => dict.Count.ToString());

            Assert.Throws<Exception>(() => _df.Select(udf(_df["info"])).Collect().ToArray());
        }

        /// <summary>
        /// UDF that returns Map type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnTypeAsMap()
        {
            Func<Column, Column> udf = Udf<string, IDictionary<string, string>>(
                str => new Dictionary<string, string> { { str, str } });

            Assert.Throws<NotImplementedException>(
                () => _df.Select(udf(_df["name"])).Collect().ToArray());
        }

        /// <summary>
        /// UDF that takes in Row type.
        /// </summary>
        [Fact]
        public void TestUdfWithRowType()
        {
            Func<Column, Column> udf = Udf<Row, string>(
                (row) =>
                {
                    string city = row.GetAs<string>("city");
                    string state = row.GetAs<string>("state");
                    return $"{city},{state}";
                });

            Row[] rows = _df.Select(udf(_df["info"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);

            var expected = new[] { "Burdwan,Paschimbanga", "Los Angeles,California", "Seattle," };
            for (int i = 0; i < rows.Length; ++i)
            {
                Row row = rows[i];
                Assert.Equal(1, row.Size());
                Assert.Equal(expected[i], row.GetAs<string>(0));
            }
        }

        /// <summary>
        /// UDF that returns Row type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnTypeAsRow()
        {
            Assert.Throws<ArgumentException>(() => Udf<string, object[]>(
                str => new object[] { 1, "abc" }));
        }
    }
}
