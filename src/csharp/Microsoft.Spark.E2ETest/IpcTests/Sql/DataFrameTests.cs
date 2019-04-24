// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class DataFrameTests
    {
        private static FieldInfo s_udfUtilsUseArrow =
            typeof(UdfUtils).GetField("s_useArrow", BindingFlags.Static | BindingFlags.NonPublic);

        private readonly SparkSession _spark;
        private readonly DataFrame _df;

        public DataFrameTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            _df = _spark
                .Read()
                .Schema("age INT, name STRING")
                .Json(TestEnvironment.ResourceDirectory + "people.json");
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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestUDF(bool useArrow)
        {
            bool originalUseArrow = GetUseArrowValue();
            SetUseArrowValue(useArrow);

            try
            {
                // Single UDF.
                Func<Column, Column, Column> udf1 = Udf<int?, string, string>(
                    (age, name) => name + " is " + (age ?? 0));
                {
                    Row[] rows = _df.Select(udf1(_df["age"], _df["name"])).Collect().ToArray();
                    Assert.Equal(3, rows.Length);
                    Assert.Equal("Michael is 0", rows[0].GetAs<string>(0));
                    Assert.Equal("Andy is 30", rows[1].GetAs<string>(0));
                    Assert.Equal("Justin is 19", rows[2].GetAs<string>(0));
                }

                // Chained UDFs.
                Func<Column, Column> udf2 = Udf<string, string>(str => $"hello {str}!");
                {
                    Row[] rows = _df
                        .Select(udf2(udf1(_df["age"], _df["name"])))
                        .Collect()
                        .ToArray();
                    Assert.Equal(3, rows.Length);
                    Assert.Equal("hello Michael is 0!", rows[0].GetAs<string>(0));
                    Assert.Equal("hello Andy is 30!", rows[1].GetAs<string>(0));
                    Assert.Equal("hello Justin is 19!", rows[2].GetAs<string>(0));
                }

                // Multiple UDFs:
                {
                    Row[] rows = _df
                        .Select(udf1(_df["age"], _df["name"]), udf2(_df["name"]))
                        .Collect()
                        .ToArray();
                    Assert.Equal(3, rows.Length);
                    Assert.Equal("Michael is 0", rows[0].GetAs<string>(0));
                    Assert.Equal("hello Michael!", rows[0].GetAs<string>(1));

                    Assert.Equal("Andy is 30", rows[1].GetAs<string>(0));
                    Assert.Equal("hello Andy!", rows[1].GetAs<string>(1));

                    Assert.Equal("Justin is 19", rows[2].GetAs<string>(0));
                    Assert.Equal("hello Justin!", rows[2].GetAs<string>(1));
                }
            }
            finally
            {
                SetUseArrowValue(originalUseArrow);
            }
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.3.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_3_X()
        {
            Column col = _df["name"];
            col = _df["age"];

            DataFrame df = _df.ToDF();
            df = df.ToDF("name2", "age2");

            StructType schema = _df.Schema();
            Assert.NotNull(schema);

            _df.PrintSchema();

            _df.Explain();
            _df.Explain(true);
            _df.Explain(false);

            Assert.Equal(2, _df.Columns().ToArray().Length);

            _df.IsLocal();

            _df.IsStreaming();

            // The following is required for *CheckPoint().
            _spark.SparkContext.SetCheckpointDir(TestEnvironment.ResourceDirectory);

            _df.Checkpoint();
            _df.Checkpoint(false);

            _df.LocalCheckpoint();
            _df.LocalCheckpoint(false);

            _df.WithWatermark("time", "10 minutes");

            _df.Show();
            _df.Show(10);
            _df.Show(10, 10);
            _df.Show(10, 10, true);

            _df.Join(_df);
            _df.Join(_df, "name");
            _df.Join(_df, new[] { "name" });
            _df.Join(_df, new[] { "name" }, "outer");
            _df.Join(_df, _df["age"] == _df["age"]);
            _df.Join(_df, _df["age"] == _df["age"], "outer");

            _df.CrossJoin(_df);

            _df.SortWithinPartitions("age");
            _df.SortWithinPartitions("age", "name");
            _df.SortWithinPartitions();
            _df.SortWithinPartitions(_df["age"]);
            _df.SortWithinPartitions(_df["age"], _df["name"]);

            _df.Sort("age");
            _df.Sort("age", "name");
            _df.Sort();
            _df.Sort(_df["age"]);
            _df.Sort(_df["age"], _df["name"]);

            _df.OrderBy("age");
            _df.OrderBy("age", "name");
            _df.OrderBy();
            _df.OrderBy(_df["age"]);
            _df.OrderBy(_df["age"], _df["name"]);

            _df.Hint("broadcast");
            _df.Hint("broadcast", new[] { "hello", "world" });

            _df.Col("age");

            _df.ColRegex("age");

            _df.As("alias");

            _df.Alias("alias");

            _df.Select("age");
            _df.Select("age", "name");
            _df.Select();
            _df.Select(_df["age"]);
            _df.Select(_df["age"], _df["name"]);

            _df.SelectExpr();
            _df.SelectExpr("age * 2");
            _df.SelectExpr("age * 2", "abs(age)");

            _df.Filter(_df["age"] > 21);
            _df.Filter("age > 21");

            _df.Where(_df["age"] > 21);
            _df.Where("age > 21");

            _df.GroupBy("age");
            _df.GroupBy("age", "name");
            _df.GroupBy();
            _df.GroupBy(_df["age"]);
            _df.GroupBy(_df["age"], _df["name"]);

            _df.Rollup("age");
            _df.Rollup("age", "name");
            _df.Rollup();
            _df.Rollup(_df["age"]);
            _df.Rollup(_df["age"], _df["name"]);

            _df.Cube("age");
            _df.Cube("age", "name");
            _df.Cube();
            _df.Cube(_df["age"]);
            _df.Cube(_df["age"], _df["name"]);

            _df.Agg(Avg(_df["age"]));
            _df.Agg(Avg(_df["age"]), Avg(_df["name"]));

            _df.Limit(10);

            _df.Union(_df);

            _df.UnionByName(_df);

            _df.Intersect(_df);

            _df.Except(_df);

            _df.Sample(0.5);
            _df.Sample(0.5, true);
            _df.Sample(0.5, false, 12345);

            _df.RandomSplit(new[] { 0.2, 0.8 });
            _df.RandomSplit(new[] { 0.2, 0.8 }, 12345);

            _df.WithColumn("age2", _df["age"]);

            _df.WithColumnRenamed("age", "age2");

            _df.Drop();
            _df.Drop("age");
            _df.Drop("age", "name");

            _df.Drop(_df["age"]);

            _df.DropDuplicates();
            _df.DropDuplicates("age");
            _df.DropDuplicates("age", "name");

            _df.Describe();
            _df.Describe("age");
            _df.Describe("age", "name");

            _df.Summary();
            _df.Summary("count");
            _df.Summary("count", "mean");

            _df.Head(2);
            _df.Head();

            _df.First();

            _df.Take(3).ToArray();

            _df.Collect().ToArray();

            _df.ToLocalIterator().ToArray();

            _df.Count();

            _df.Repartition(2);
            _df.Repartition(2, _df["age"]);
            _df.Repartition(_df["age"]);
            _df.Repartition();

            _df.RepartitionByRange(2, _df["age"]);
            _df.RepartitionByRange(_df["age"]);

            _df.Coalesce(1);

            _df.Distinct();

            _df.Persist();

            _df.Cache();

            _df.Unpersist();

            _df.CreateTempView("view");
            _df.CreateOrReplaceTempView("view");

            _df.CreateGlobalTempView("global_view");
            _df.CreateOrReplaceGlobalTempView("global_view");
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 2.4.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestSignaturesV2_4_X()
        {
            _df.IsEmpty();

            _df.IntersectAll(_df);

            _df.ExceptAll(_df);
        }

        private static bool GetUseArrowValue()
        {
            return (bool)s_udfUtilsUseArrow.GetValue(null);
        }

        private static void SetUseArrowValue(bool value)
        {
            s_udfUtilsUseArrow.SetValue(null, value);
        }
    }
}
