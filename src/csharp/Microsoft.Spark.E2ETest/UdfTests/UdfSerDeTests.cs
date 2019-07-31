// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.E2ETest.ExternalLibrary;
using Microsoft.Spark.Sql;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.UdfTests
{
    [Collection("Spark E2E Tests")]
    public class UdfSerDeTests
    {
        private readonly SparkSession _spark;
        private readonly DataFrame _df;

        public UdfSerDeTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            _df = _spark
                .Read()
                .Schema("age INT, name STRING")
                .Json($"{TestEnvironment.ResourceDirectory}people.json");
        }

        [Fact]
        public void TestUdfClosure()
        {
            var ec = new ExternalClass("Hello");
            Func<Column, Column> udf = Udf<string, string>(
                (str) =>
                {
                    return ec.Concat(str);
                });

            Row[] rows = _df.Select(udf(_df["name"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);

            var expected = new[] { "HelloMichael", "HelloAndy", "HelloJustin" };
            for (int i = 0; i < rows.Length; ++i)
            {
                Row row = rows[i];
                Assert.Equal(1, row.Size());
                Assert.Equal(expected[i], row.GetAs<string>(0));
            }
        }

        [Fact]
        public void TestExternalStaticMethodCall()
        {
            Func<Column, Column> udf = Udf<string, string>(str =>
            {
                return ExternalClass.HelloWorld();
            });

            Row[] rows = _df.Select(udf(_df["name"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);

            for (int i = 0; i < rows.Length; ++i)
            {
                Row row = rows[i];
                Assert.Equal(1, row.Size());
                Assert.Equal("Hello World", row.GetAs<string>(0));
            }
        }

        [Fact]
        public void TestInitExternalClassInUdf()
        {
            // Instantiate external assembly class within body of Udf.
            Func<Column, Column> udf = Udf<string, string>(
                    (str) =>
                    {
                        var ec = new ExternalClass("Hello");
                        return ec.Concat(str);
                    });

            Row[] rows = _df.Select(udf(_df["name"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);

            var expected = new[] { "HelloMichael", "HelloAndy", "HelloJustin" };
            for (int i = 0; i < rows.Length; ++i)
            {
                Row row = rows[i];
                Assert.Equal(1, row.Size());
                Assert.Equal(expected[i], row.GetAs<string>(0));
            }
        }
    }
}
