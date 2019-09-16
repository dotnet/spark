// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
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
                .Json($"{TestEnvironment.ResourceDirectory}people_array.json");
        }

        // UDF that takes in Array type.
        [Fact]
        public void TestUdfWithArrayType()
        {
            //ArrayList works for this type.
            Func<Column, Column> udfInt = Udf<int[], string>(
                    (arr) =>
                    {
                        return string.Join(',', arr);
                    });

            Assert.Throws<Exception>(() => _df.Select(udfInt(_df["ages"])).Collect().ToArray());

            Func<Column, Column> udfString = Udf<string[], string>(
                    (arr) =>
                    {
                        return string.Join(',', arr);
                    });

            Assert.Throws<Exception>(() => _df.Select(udfString(_df["cars"])).Collect().ToArray());
        }

        // UDF that returns Array type.
        [Fact]
        public void TestUdfWithReturnTypeAsArray()
        {
            Func<Column, Column> udf = Udf<string, string[]>(
                    (str) =>
                    {
                        return new string[] { str, str + str };
                    });

            Assert.Throws<NotImplementedException>(() => _df.Select(udf(_df["name"])).Collect().ToArray());
        }

        // UDF that takes in Map type.
        [Fact]
        public void TestUdfWithMapType()
        {
            Func<Column, Column> udf = Udf<IDictionary<string, string>, string>(
                    (dict) =>
                    {
                        return dict.Count.ToString();
                    });

            Assert.Throws<Exception>(() => _df.Select(udf(_df["info"])).Collect().ToArray());
        }

        // UDF that returns Map type.
        [Fact]
        public void TestUdfWithReturnTypeAsMap()
        {
            Func<Column, Column> udf = Udf<string, IDictionary<string, string>>(
                (str) =>
                {
                    return new Dictionary<string, string> { { str, str } };
                });

            Assert.Throws<NotImplementedException>(() => _df.Select(udf(_df["name"])).Collect().ToArray());
        }

        // UDF that takes in Row type.
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

        // UDF that returns Row type.
        [Fact]
        public void TestUdfWithReturnTypeAsRow()
        {
            Assert.Throws<ArgumentException>(() => Udf<string, object[]>(
                (str) =>
                {
                    return new object[] { 1, "abc" };
                }));
        }
    }
}
