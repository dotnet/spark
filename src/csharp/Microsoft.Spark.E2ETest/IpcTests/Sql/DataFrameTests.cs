// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using Apache.Arrow;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using static Microsoft.Spark.Sql.Functions;
using static Microsoft.Spark.UnitTest.TestUtils.ArrowTestUtils;
using Int32Type = Apache.Arrow.Types.Int32Type;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class DataFrameTests
    {
        private readonly SparkSession _spark;
        private readonly DataFrame _df;

        public DataFrameTests(SparkFixture fixture)
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
        public void TestUDF()
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

        [Fact]
        public void TestVectorUdf()
        {
            Func<Int32Array, StringArray, StringArray> udf1Func =
                (ages, names) => (StringArray)ToArrowArray(
                    Enumerable.Range(0, names.Length)
                        .Select(i => $"{names.GetString(i)} is {ages.GetValue(i) ?? 0}")
                        .ToArray());

            // Single UDF.
            Func<Column, Column, Column> udf1 =
                ExperimentalFunctions.VectorUdf(udf1Func);
            {
                Row[] rows = _df.Select(udf1(_df["age"], _df["name"])).Collect().ToArray();
                Assert.Equal(3, rows.Length);
                Assert.Equal("Michael is 0", rows[0].GetAs<string>(0));
                Assert.Equal("Andy is 30", rows[1].GetAs<string>(0));
                Assert.Equal("Justin is 19", rows[2].GetAs<string>(0));
            }

            // Chained UDFs.
            Func<Column, Column> udf2 = ExperimentalFunctions.VectorUdf<StringArray, StringArray>(
                (strings) => (StringArray)ToArrowArray(
                    Enumerable.Range(0, strings.Length)
                        .Select(i => $"hello {strings.GetString(i)}!")
                        .ToArray()));
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

            // Register UDF
            {
                _df.CreateOrReplaceTempView("people");
                _spark.Udf().RegisterVector("udf1", udf1Func);
                Row[] rows = _spark.Sql("SELECT udf1(age, name) FROM people")
                    .Collect()
                    .ToArray();
                Assert.Equal(3, rows.Length);
                Assert.Equal("Michael is 0", rows[0].GetAs<string>(0));
                Assert.Equal("Andy is 30", rows[1].GetAs<string>(0));
                Assert.Equal("Justin is 19", rows[2].GetAs<string>(0));
            }
        }

        [Fact]
        public void TestGroupedMapUdf()
        {
            DataFrame df = _spark
                .Read()
                .Schema("age INT, name STRING")
                .Json($"{TestEnvironment.ResourceDirectory}more_people.json");
            // Data:
            // { "name":"Michael"}
            // { "name":"Andy", "age":30}
            // { "name":"Seth", "age":30}
            // { "name":"Justin", "age":19}
            // { "name":"Kathy", "age":19}

            Row[] rows = df.GroupBy("age")
                .Apply(
                    new StructType(new[]
                    {
                        new StructField("age", new IntegerType()),
                        new StructField("nameCharCount", new IntegerType())
                    }),
                    batch => CountCharacters(batch))
                .Collect()
                .ToArray();

            Assert.Equal(3, rows.Length);
            foreach (Row row in rows)
            {
                int age = row.GetAs<int>("age");
                int? charCount = row.GetAs<int?>("nameCharCount");
                switch (age)
                {
                    case 0:
                        // The results here are incorrect for the {name: "Michael" age: null} 
                        // record because of https://issues.apache.org/jira/browse/ARROW-5887.
                        // When an updated Apache.Arrow library is available with the fix,
                        // this should change to check for age: null, charCount: 7.
                        Assert.Null(charCount);
                        break;
                    case 19:
                        Assert.Equal(11, charCount);
                        break;
                    case 30:
                        Assert.Equal(8, charCount);
                        break;
                    default:
                        throw new Exception($"Unexpected age: {age}.");
                }
            }
        }

        private static RecordBatch CountCharacters(RecordBatch records)
        {
            int stringFieldIndex = records.Schema.GetFieldIndex("name");
            StringArray stringValues = records.Column(stringFieldIndex) as StringArray;

            int characterCount = 0;

            for (int i = 0; i < stringValues.Length; ++i)
            {
                string current = stringValues.GetString(i);
                characterCount += current.Length;
            }

            int groupFieldIndex = records.Schema.GetFieldIndex("age");
            Field groupField = records.Schema.GetFieldByIndex(groupFieldIndex);

            // Return 1 record, if we were given any. 0, otherwise.
            int returnLength = records.Length > 0 ? 1 : 0;

            return new RecordBatch(
                new Schema.Builder()
                    .Field(f => f.Name(groupField.Name).DataType(groupField.DataType))
                    .Field(f => f.Name("name_CharCount").DataType(Int32Type.Default))
                    .Build(),
                new IArrowArray[]
                {
                    records.Column(groupFieldIndex),
                    new Int32Array(
                        new ArrowBuffer.Builder<int>().Append(characterCount).Build(),
                        ArrowBuffer.Empty,
                        length: 1,
                        nullCount: 0,
                        offset: 0)
                },
                returnLength);
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

            using (var tempDir = new TemporaryDirectory())
            {
                // The following is required for *CheckPoint().
                _spark.SparkContext.SetCheckpointDir(tempDir.Path);

                _df.Checkpoint();
                _df.Checkpoint(false);

                _df.LocalCheckpoint();
                _df.LocalCheckpoint(false);
            }

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
    }
}
