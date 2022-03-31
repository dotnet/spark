// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;
using static Microsoft.Spark.Sql.ArrowFunctions;
using static Microsoft.Spark.Sql.DataFrameFunctions;
using static Microsoft.Spark.Sql.Functions;
using static Microsoft.Spark.UnitTest.TestUtils.ArrowTestUtils;
using Column = Microsoft.Spark.Sql.Column;
using DataFrame = Microsoft.Spark.Sql.DataFrame;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;
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
        public void TestWithColumn()
        {
            Func<Column, Column> sizeNameAgeUdf = Udf<Row, string>(
                r =>
                {
                    string name = r.GetAs<string>("name");
                    int? age = r.GetAs<int?>("age");
                    if (age.HasValue)
                    {
                        return $"{r.Size()},{name},{age.Value}";
                    }

                    return $"{r.Size()},{name},{string.Empty}";
                });

            string[] allCols = _df.Columns().ToArray();
            DataFrame nameAgeColDF =
                _df.WithColumn("NameAgeCol", Struct(allCols[0], allCols.Skip(1).ToArray()));
            DataFrame sizeNameAgeColDF =
                nameAgeColDF.WithColumn("SizeNameAgeCol", sizeNameAgeUdf(nameAgeColDF["NameAgeCol"]));

            Row[] originalDFRows = _df.Collect().ToArray();
            Assert.Equal(3, originalDFRows.Length);

            Row[] sizeNameAgeColDFRows = sizeNameAgeColDF.Collect().ToArray();
            Assert.Equal(3, sizeNameAgeColDFRows.Length);

            {
                Row row = sizeNameAgeColDFRows[0];
                Assert.Equal("Michael", row.GetAs<string>("name"));
                Assert.Null(row.Get("age"));
                Assert.IsType<Row>(row.Get("NameAgeCol"));
                Assert.Equal(originalDFRows[0], row.GetAs<Row>("NameAgeCol"));
                Assert.Equal("2,Michael,", row.GetAs<string>("SizeNameAgeCol"));
            }

            {
                Row row = sizeNameAgeColDFRows[1];
                Assert.Equal("Andy", row.GetAs<string>("name"));
                Assert.Equal(30, row.GetAs<int>("age"));
                Assert.IsType<Row>(row.Get("NameAgeCol"));
                Assert.Equal(originalDFRows[1], row.GetAs<Row>("NameAgeCol"));
                Assert.Equal("2,Andy,30", row.GetAs<string>("SizeNameAgeCol"));
            }

            {
                Row row = sizeNameAgeColDFRows[2];
                Assert.Equal("Justin", row.GetAs<string>("name"));
                Assert.Equal(19, row.GetAs<int>("age"));
                Assert.IsType<Row>(row.Get("NameAgeCol"));
                Assert.Equal(originalDFRows[2], row.GetAs<Row>("NameAgeCol"));
                Assert.Equal("2,Justin,19", row.GetAs<string>("SizeNameAgeCol"));
            }
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
            Func<Column, Column, Column> udf1 = VectorUdf(udf1Func);
            {
                Row[] rows = _df.Select(udf1(_df["age"], _df["name"])).Collect().ToArray();
                Assert.Equal(3, rows.Length);
                Assert.Equal("Michael is 0", rows[0].GetAs<string>(0));
                Assert.Equal("Andy is 30", rows[1].GetAs<string>(0));
                Assert.Equal("Justin is 19", rows[2].GetAs<string>(0));
            }

            // Chained UDFs.
            Func<Column, Column> udf2 = VectorUdf<StringArray, StringArray>(
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
        public void TestDataFrameVectorUdf()
        {
            Func<Int32DataFrameColumn, ArrowStringDataFrameColumn, ArrowStringDataFrameColumn> udf1Func =
                (ages, names) =>
                {
                    long i = 0;
                    return names.Apply(cur => $"{cur} is {ages[i++] ?? 0}");
                };

            // Single UDF.
            Func<Column, Column, Column> udf1 = VectorUdf(udf1Func);
            {
                Row[] rows = _df.Select(udf1(_df["age"], _df["name"])).Collect().ToArray();
                Assert.Equal(3, rows.Length);
                Assert.Equal("Michael is 0", rows[0].GetAs<string>(0));
                Assert.Equal("Andy is 30", rows[1].GetAs<string>(0));
                Assert.Equal("Justin is 19", rows[2].GetAs<string>(0));
            }

            // Chained UDFs.
            Func<Column, Column> udf2 = VectorUdf<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                (strings) => strings.Apply(cur => $"hello {cur}!"));
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
                    batch => ArrowBasedCountCharacters(batch))
                .Collect()
                .ToArray();

            Assert.Equal(3, rows.Length);
            foreach (Row row in rows)
            {
                int? age = row.GetAs<int?>("age");
                int charCount = row.GetAs<int>("nameCharCount");
                switch (age)
                {
                    case null:
                        Assert.Equal(7, charCount);
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

        private static RecordBatch ArrowBasedCountCharacters(RecordBatch records)
        {
            StringArray nameColumn = records.Column("name") as StringArray;

            int characterCount = 0;

            for (int i = 0; i < nameColumn.Length; ++i)
            {
                string current = nameColumn.GetString(i);
                characterCount += current.Length;
            }

            int ageFieldIndex = records.Schema.GetFieldIndex("age");
            Field ageField = records.Schema.GetFieldByIndex(ageFieldIndex);

            // Return 1 record, if we were given any. 0, otherwise.
            int returnLength = records.Length > 0 ? 1 : 0;

            return new RecordBatch(
                new Schema.Builder()
                    .Field(ageField)
                    .Field(f => f.Name("name_CharCount").DataType(Int32Type.Default))
                    .Build(),
                new IArrowArray[]
                {
                    records.Column(ageFieldIndex),
                    new Int32Array.Builder().Append(characterCount).Build()
                },
                returnLength);
        }

        [Fact]
        public void TestDataFrameGroupedMapUdf()
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
                int? age = row.GetAs<int?>("age");
                int charCount = row.GetAs<int>("nameCharCount");
                switch (age)
                {
                    case null:
                        Assert.Equal(7, charCount);
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

        private static FxDataFrame CountCharacters(FxDataFrame dataFrame)
        {
            int characterCount = 0;

            var characterCountColumn = new Int32DataFrameColumn("nameCharCount");
            var ageColumn = new Int32DataFrameColumn("age");
            ArrowStringDataFrameColumn fieldColumn = dataFrame.Columns.GetArrowStringColumn("name");
            for (long i = 0; i < dataFrame.Rows.Count; ++i)
            {
                characterCount += fieldColumn[i].Length;
            }

            if (dataFrame.Rows.Count > 0)
            {
                characterCountColumn.Append(characterCount);
                ageColumn.Append(dataFrame.Columns.GetInt32Column("age")[0]);
            }

            return new FxDataFrame(ageColumn, characterCountColumn);
        }

        /// <summary>
        /// Test signatures for APIs up to Spark 2.4.*.
        /// </summary>
        [Fact]
        public void TestSignaturesV2_4_X()
        {
            Assert.IsType<Column>(_df["name"]);
            Assert.IsType<Column>(_df["age"]);

            Assert.IsType<DataFrame>(_df.ToDF());
            Assert.IsType<DataFrame>(_df.ToDF("name2", "age2"));

            StructType schema = _df.Schema();
            Assert.NotNull(schema);

            _df.PrintSchema();

            _df.Explain();
            _df.Explain(true);
            _df.Explain(false);

            Assert.Equal(2, _df.Columns().ToArray().Length);

            var expected = new List<Tuple<string, string>>
            {
                new Tuple<string, string>("age", "integer"),
                new Tuple<string, string>("name", "string")
            };
            Assert.Equal(expected, _df.DTypes());

            Assert.IsType<bool>(_df.IsLocal());

            Assert.IsType<bool>(_df.IsStreaming());

            using (var tempDir = new TemporaryDirectory())
            {
                // The following is required for *CheckPoint().
                _spark.SparkContext.SetCheckpointDir(tempDir.Path);

                Assert.IsType<DataFrame>(_df.Checkpoint());
                Assert.IsType<DataFrame>(_df.Checkpoint(false));

                Assert.IsType<DataFrame>(_df.LocalCheckpoint());
                Assert.IsType<DataFrame>(_df.LocalCheckpoint(false));
            }

            Assert.IsType<DataFrame>(_df.WithWatermark("time", "10 minutes"));

            _df.Show();
            _df.Show(10);
            _df.Show(10, 10);
            _df.Show(10, 10, true);

            Assert.IsType<DataFrame>(_df.ToJSON());

            Assert.IsType<DataFrame>(_df.Join(_df));
            Assert.IsType<DataFrame>(_df.Join(_df, "name"));
            Assert.IsType<DataFrame>(_df.Join(_df, new[] { "name" }));
            Assert.IsType<DataFrame>(_df.Join(_df, new[] { "name" }, "outer"));
            Assert.IsType<DataFrame>(_df.Join(_df, _df["age"] == _df["age"]));
            Assert.IsType<DataFrame>(_df.Join(_df, _df["age"] == _df["age"], "outer"));

            Assert.IsType<DataFrame>(_df.CrossJoin(_df));

            Assert.IsType<DataFrame>(_df.SortWithinPartitions("age"));
            Assert.IsType<DataFrame>(_df.SortWithinPartitions("age", "name"));
            Assert.IsType<DataFrame>(_df.SortWithinPartitions());
            Assert.IsType<DataFrame>(_df.SortWithinPartitions(_df["age"]));
            Assert.IsType<DataFrame>(_df.SortWithinPartitions(_df["age"], _df["name"]));

            Assert.IsType<DataFrame>(_df.Sort("age"));
            Assert.IsType<DataFrame>(_df.Sort("age", "name"));
            Assert.IsType<DataFrame>(_df.Sort());
            Assert.IsType<DataFrame>(_df.Sort(_df["age"]));
            Assert.IsType<DataFrame>(_df.Sort(_df["age"], _df["name"]));

            Assert.IsType<DataFrame>(_df.OrderBy("age"));
            Assert.IsType<DataFrame>(_df.OrderBy("age", "name"));
            Assert.IsType<DataFrame>(_df.OrderBy());
            Assert.IsType<DataFrame>(_df.OrderBy(_df["age"]));
            Assert.IsType<DataFrame>(_df.OrderBy(_df["age"], _df["name"]));

            Assert.IsType<DataFrame>(_df.Hint("broadcast"));
            Assert.IsType<DataFrame>(_df.Hint("broadcast", new[] { "hello", "world" }));

            Assert.IsType<Column>(_df.Col("age"));

            Assert.IsType<Column>(_df.ColRegex("age"));

            Assert.IsType<DataFrame>(_df.As("alias"));

            Assert.IsType<DataFrame>(_df.Alias("alias"));

            Assert.IsType<DataFrame>(_df.Select("age"));
            Assert.IsType<DataFrame>(_df.Select("age", "name"));
            Assert.IsType<DataFrame>(_df.Select());
            Assert.IsType<DataFrame>(_df.Select(_df["age"]));
            Assert.IsType<DataFrame>(_df.Select(_df["age"], _df["name"]));

            Assert.IsType<DataFrame>(_df.SelectExpr());
            Assert.IsType<DataFrame>(_df.SelectExpr("age * 2"));
            Assert.IsType<DataFrame>(_df.SelectExpr("age * 2", "abs(age)"));

            Assert.IsType<DataFrame>(_df.Filter(_df["age"] > 21));
            Assert.IsType<DataFrame>(_df.Filter("age > 21"));

            Assert.IsType<DataFrame>(_df.Where(_df["age"] > 21));
            Assert.IsType<DataFrame>(_df.Where("age > 21"));

            Assert.IsType<RelationalGroupedDataset>(_df.GroupBy("age"));
            Assert.IsType<RelationalGroupedDataset>(_df.GroupBy("age", "name"));
            Assert.IsType<RelationalGroupedDataset>(_df.GroupBy());
            Assert.IsType<RelationalGroupedDataset>(_df.GroupBy(_df["age"]));
            Assert.IsType<RelationalGroupedDataset>(_df.GroupBy(_df["age"], _df["name"]));

            {
                RelationalGroupedDataset df =
                    _df.WithColumn("tempAge", _df["age"]).GroupBy("name");

                Assert.IsType<DataFrame>(df.Mean("age"));
                Assert.IsType<DataFrame>(df.Mean("age", "tempAge"));

                Assert.IsType<DataFrame>(df.Max("age"));
                Assert.IsType<DataFrame>(df.Max("age", "tempAge"));

                Assert.IsType<DataFrame>(df.Avg("age"));
                Assert.IsType<DataFrame>(df.Avg("age", "tempAge"));

                Assert.IsType<DataFrame>(df.Min("age"));
                Assert.IsType<DataFrame>(df.Min("age", "tempAge"));

                Assert.IsType<DataFrame>(df.Sum("age"));
                Assert.IsType<DataFrame>(df.Sum("age", "tempAge"));

                var values = new List<object> { 19, "twenty" };

                Assert.IsType<RelationalGroupedDataset>(df.Pivot("age"));

                Assert.IsType<RelationalGroupedDataset>(df.Pivot(Col("age")));

                Assert.IsType<RelationalGroupedDataset>(df.Pivot("age", values));

                Assert.IsType<RelationalGroupedDataset>(df.Pivot(Col("age"), values));
            }

            Assert.IsType<RelationalGroupedDataset>(_df.Rollup("age"));
            Assert.IsType<RelationalGroupedDataset>(_df.Rollup("age", "name"));
            Assert.IsType<RelationalGroupedDataset>(_df.Rollup());
            Assert.IsType<RelationalGroupedDataset>(_df.Rollup(_df["age"]));
            Assert.IsType<RelationalGroupedDataset>(_df.Rollup(_df["age"], _df["name"]));

            Assert.IsType<RelationalGroupedDataset>(_df.Cube("age"));
            Assert.IsType<RelationalGroupedDataset>(_df.Cube("age", "name"));
            Assert.IsType<RelationalGroupedDataset>(_df.Cube());
            Assert.IsType<RelationalGroupedDataset>(_df.Cube(_df["age"]));
            Assert.IsType<RelationalGroupedDataset>(_df.Cube(_df["age"], _df["name"]));

            Assert.IsType<DataFrame>(_df.Agg(Avg(_df["age"])));
            Assert.IsType<DataFrame>(_df.Agg(Avg(_df["age"]), Avg(_df["name"])));

            Assert.IsType<DataFrame>(_df.Limit(10));

            Assert.IsType<DataFrame>(_df.Union(_df));

            Assert.IsType<DataFrame>(_df.UnionByName(_df));

            Assert.IsType<DataFrame>(_df.Intersect(_df));

            Assert.IsType<DataFrame>(_df.Except(_df));

            Assert.IsType<DataFrame>(_df.Sample(0.5));
            Assert.IsType<DataFrame>(_df.Sample(0.5, true));
            Assert.IsType<DataFrame>(_df.Sample(0.5, false, 12345));

            Assert.IsType<DataFrame[]>(_df.RandomSplit(new[] { 0.2, 0.8 }));
            Assert.IsType<DataFrame[]>(_df.RandomSplit(new[] { 0.2, 0.8 }, 12345));

            Assert.IsType<DataFrame>(_df.WithColumn("age2", _df["age"]));

            Assert.IsType<DataFrame>(_df.WithColumnRenamed("age", "age2"));

            Assert.IsType<DataFrame>(_df.Drop());
            Assert.IsType<DataFrame>(_df.Drop("age"));
            Assert.IsType<DataFrame>(_df.Drop("age", "name"));

            Assert.IsType<DataFrame>(_df.Drop(_df["age"]));

            Assert.IsType<DataFrame>(_df.DropDuplicates());
            Assert.IsType<DataFrame>(_df.DropDuplicates("age"));
            Assert.IsType<DataFrame>(_df.DropDuplicates("age", "name"));

            Assert.IsType<DataFrame>(_df.Describe());
            Assert.IsType<DataFrame>(_df.Describe("age"));
            Assert.IsType<DataFrame>(_df.Describe("age", "name"));

            Assert.IsType<DataFrame>(_df.Summary());
            Assert.IsType<DataFrame>(_df.Summary("count"));
            Assert.IsType<DataFrame>(_df.Summary("count", "mean"));

            Assert.IsType<Row[]>(_df.Head(2).ToArray());
            Assert.IsType<Row>(_df.Head());

            Assert.IsType<Row>(_df.First());

            Assert.IsType<DataFrame>(_df.Transform(df => df.Drop("age")));

            Assert.IsType<Row[]>(_df.Take(3).ToArray());

            Assert.IsType<Row[]>(_df.Collect().ToArray());

            Assert.IsType<Row[]>(_df.ToLocalIterator().ToArray());

            Assert.IsType<long>(_df.Count());

            Assert.IsType<DataFrame>(_df.Repartition(2));
            Assert.IsType<DataFrame>(_df.Repartition(2, _df["age"]));
            Assert.IsType<DataFrame>(_df.Repartition(_df["age"]));
            Assert.IsType<DataFrame>(_df.Repartition());

            Assert.IsType<DataFrame>(_df.RepartitionByRange(2, _df["age"]));
            Assert.IsType<DataFrame>(_df.RepartitionByRange(_df["age"]));

            Assert.IsType<DataFrame>(_df.Coalesce(1));

            Assert.IsType<DataFrame>(_df.Distinct());

            Assert.IsType<DataFrame>(_df.Persist());

            Assert.IsType<DataFrame>(_df.Persist(StorageLevel.DISK_ONLY));

            Assert.IsType<DataFrame>(_df.Cache());

            Assert.IsType<StorageLevel>(_df.StorageLevel());

            Assert.IsType<DataFrame>(_df.Unpersist());

            _df.CreateTempView("view");
            _df.CreateOrReplaceTempView("view");

            _df.CreateGlobalTempView("global_view");
            _df.CreateOrReplaceGlobalTempView("global_view");

            Assert.IsType<string[]>(_df.InputFiles().ToArray());

            _df.IsEmpty();

            _df.IntersectAll(_df);

            _df.ExceptAll(_df);
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.0.*.

        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_0_0)]
        public void TestSignaturesV3_0_X()
        {
            // Validate ToLocalIterator
            var data = new List<GenericRow>
            {
                new GenericRow(new object[] { "Alice", 20}),
                new GenericRow(new object[] { "Bob", 30})
            };
            var schema = new StructType(new List<StructField>()
            {
                new StructField("Name", new StringType()),
                new StructField("Age", new IntegerType())
            });
            DataFrame df = _spark.CreateDataFrame(data, schema);
            IEnumerable<Row> actual = df.ToLocalIterator(true).ToArray();
            IEnumerable<Row> expected = data.Select(r => new Row(r.Values, schema));
            Assert.Equal(expected, actual);

            Assert.IsType<DataFrame>(df.Observe("metrics", Count("Name").As("CountNames")));

            Assert.IsType<Row[]>(_df.Tail(1).ToArray());

            _df.PrintSchema(1);

            _df.Explain("simple");
            _df.Explain("extended");
            _df.Explain("codegen");
            _df.Explain("cost");
            _df.Explain("formatted");
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.1.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_1_0)]
        public void TestSignaturesV3_1_X()
        {
            Assert.IsType<DataFrame>(_df.UnionByName(_df, true));

            Assert.IsType<bool>(_df.SameSemantics(_df));

            Assert.IsType<int>(_df.SemanticHash());
        }
    }
}
