// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
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
                .Json(Path.Combine($"{TestEnvironment.ResourceDirectory}people.json"));
        }

        /// <summary>
        /// UDF that takes in Array type.
        /// </summary>
        [Fact]
        public void TestUdfWithSimpleArrayType()
        {
            var schema = new StructType(new StructField[]
            {
                new StructField("name", new StringType()),
                new StructField("ids", new ArrayType(new IntegerType()))
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[] { "Name1", new int[] { 1, 2, 3 } }),
                new GenericRow(new object[] { "Name2", null }),
                new GenericRow(new object[] { "Name3", new int[] { 4 } }),
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);

            var expected = new string[] { "Name1|1,2,3", "Name2", "Name3|4" };

            {
                // Test using array
                Func<Column, Column, Column> udf =
                    Udf<string, int[], string>(
                        (name, ids) =>
                        {
                            if (ids == null)
                            {
                                return name;
                            }

                            return AppendEnumerable(name, ids);
                        });

                Row[] rows = df.Select(udf(df["name"], df["ids"])).Collect().ToArray();
                Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));
            }
            {
                // Test using ArrayList
                Func<Column, Column, Column> udf =
                    Udf<string, ArrayList, string>(
                        (name, ids) =>
                        {
                            if (ids == null)
                            {
                                return name;
                            }

                            return AppendEnumerable(name, ids.ToArray());
                        });

                Row[] rows = df.Select(udf(df["name"], df["ids"])).Collect().ToArray();
                Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));
            }
        }

        [Fact]
        public void TestUdfWithArrayOfArrayType()
        {
            var schema = new StructType(new StructField[]
            {
                new StructField("name", new StringType()),
                new StructField("ids", new ArrayType(new IntegerType())),
                new StructField("arrIds", new ArrayType(new ArrayType(new IntegerType()))),
                new StructField(
                    "arrArrIds",
                    new ArrayType(new ArrayType(new ArrayType(new IntegerType()))))
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[]
                {
                    "Name1",
                    new int[] { 1, 2, 3 },
                    new int[][]
                    {
                        new int[] { 10, 11 },
                        new int[] { 12 }
                    },
                    new int[][][]
                    {
                        new int[][]
                        {
                            new int[] { 100, 101 },
                            new int[] { 102 }
                        },
                        new int[][]
                        {
                            new int[] { 103 }
                        }
                    }
                }),
                new GenericRow(new object[]
                {
                    "Name2",
                    null,
                    null,
                    null
                }),
                new GenericRow(new object[]
                {
                    "Name3",
                    new int[] { 4 },
                    new int[][]
                    {
                        new int[] { 13 },
                    },
                    new int[][][]
                    {
                        new int[][]
                        {
                            new int[] { 104 }
                        }
                    }
                }),
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);

            var expected = new string[]
            {
                "Name1|1,2,3|+10,+11,+12|++100,++101,++102,++103",
                "Name2",
                "Name3|4|+13|++104"
            };

            {
                // Test using array
                Func<Column, Column, Column, Column, Column> udf =
                    Udf<string, int[], int[][], int[][][], string>(
                        (name, ids, arrIds, arrArrIds) =>
                        {
                            var sb = new StringBuilder();
                            sb.Append(name);

                            if (ids != null)
                            {
                                AppendEnumerable(sb, ids);
                            }

                            if (arrIds != null)
                            {
                                AppendEnumerable(
                                    sb,
                                    arrIds.SelectMany(i => i.Select(j => $"+{j}")));
                            }

                            if (arrArrIds != null)
                            {
                                AppendEnumerable(
                                    sb,
                                    arrArrIds.SelectMany(
                                        i => i.SelectMany(j => j.Select(k => $"++{k}"))));
                            }

                            return sb.ToString();
                        });

                Row[] rows =
                    df.Select(udf(df["name"], df["ids"], df["arrIds"], df["arrArrIds"]))
                    .Collect()
                    .ToArray();

                Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));
            }
            {
                // Test using ArrayList
                Func<Column, Column, Column, Column, Column> udf =
                    Udf<string, ArrayList, ArrayList, ArrayList, string>(
                        (name, ids, arrIds, arrArrIds) =>
                        {
                            var sb = new StringBuilder();
                            sb.Append(name);

                            if (ids != null)
                            {
                                AppendEnumerable(sb, ids.ToArray());
                            }

                            if (arrIds != null)
                            {
                                IEnumerable<object> idsEnum = arrIds
                                    .ToArray()
                                    .SelectMany(
                                        i => ((ArrayList)i).ToArray().Select(j => $"+{j}"));
                                AppendEnumerable(sb, idsEnum);
                            }

                            if (arrArrIds != null)
                            {
                                IEnumerable<object> idsEnum = arrArrIds
                                    .ToArray()
                                    .SelectMany(i => ((ArrayList)i).ToArray()
                                        .SelectMany(
                                            j => ((ArrayList)j).ToArray().Select(k => $"++{k}")));
                                AppendEnumerable(sb, idsEnum);
                            }

                            return sb.ToString();
                        });

                Row[] rows =
                    df.Select(udf(df["name"], df["ids"], df["arrIds"], df["arrArrIds"]))
                    .Collect()
                    .ToArray();

                Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));
            }
        }

        [Fact]
        public void TestUdfWithRowArrayType()
        {
            // Test array of Rows
            var schema = new StructType(new StructField[]
            {
                new StructField("name", new StringType()),
                new StructField("rows", new ArrayType(
                    new StructType(new StructField[]
                    {
                        new StructField("first", new StringType()),
                        new StructField("second", new StringType()),
                        new StructField("ids", new ArrayType(new IntegerType())),
                    })))
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[]
                {
                    "Name1",
                    new GenericRow[]
                    {
                        new GenericRow(new object[]
                        {
                            "f1",
                            "s1",
                            new int[] { 1, 2, 3 }
                        }),
                        new GenericRow(new object[]
                        {
                            "f2",
                            "s2",
                            new int[] { 4, 5 }
                        })
                    }
                }),
                new GenericRow(new object[]
                {
                    "Name2",
                    null,
                }),
                new GenericRow(new object[]
                {
                    "Name3",
                    new GenericRow[]
                    {
                        new GenericRow(new object[]
                        {
                            "f3",
                            "s3",
                            new int[] { 6 }
                        })
                    }
                }),
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);

            var expected = new string[]
            {
                "Name1|f1s1,1,2,3,f2s2,4,5",
                "Name2",
                "Name3|f3s3,6"
            };

            {
                // Test using array
                Func<Column, Column, Column> udf =
                    Udf<string, Row[], string>(
                        (name, rows) =>
                        {
                            var sb = new StringBuilder();
                            sb.Append(name);

                            if (rows != null)
                            {
                                AppendEnumerable(sb, rows.Select(r =>
                                {
                                    string firstlast =
                                        r.GetAs<string>(0) + r.GetAs<string>(1);
                                    int[] ids = r.GetAs<int[]>("ids");
                                    if (ids == null)
                                    {
                                        return firstlast;
                                    }

                                    return firstlast + "," + string.Join(",", ids);
                                }));
                            }

                            return sb.ToString();
                        });

                Row[] rows = df.Select(udf(df["name"], df["rows"])).Collect().ToArray();
                Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));
            }

            {
                // Test using ArrayList
                Func<Column, Column, Column> udf =
                    Udf<string, ArrayList, string>(
                        (name, rows) =>
                        {
                            var sb = new StringBuilder();
                            sb.Append(name);

                            if (rows != null)
                            {
                                AppendEnumerable(sb, rows.ToArray().Select(r =>
                                {
                                    Row row = (Row)r;
                                    string firstlast =
                                        row.GetAs<string>(0) + row.GetAs<string>(1);
                                    int[] ids = row.GetAs<int[]>("ids");
                                    if (ids == null)
                                    {
                                        return firstlast;
                                    }

                                    return firstlast + "," + string.Join(",", ids);
                                }));
                            }

                            return sb.ToString();
                        });

                Row[] rows = df.Select(udf(df["name"], df["rows"])).Collect().ToArray();
                Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));
            }
        }

        /// <summary>
        /// UDF that returns Array type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsSimpleArrayType()
        {
            // Test simple arrays
            var schema = new StructType(new StructField[]
            {
                new StructField("first", new StringType()),
                new StructField("last",  new StringType())
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[] { "John", "Smith" }),
                new GenericRow(new object[] { "Jane", "Doe" })
            };

            Func<Column, Column, Column> udf =
                Udf<string, string, string[]>((first, last) => new string[] { first, last });

            DataFrame df = _spark.CreateDataFrame(data, schema);
            Row[] rows = df.Select(udf(df["first"], df["last"])).Collect().ToArray();

            var expected = new string[][]
            {
                new[] { "John", "Smith" },
                new[] { "Jane", "Doe" }
            };

            Assert.Equal(expected.Length, rows.Length);

            for (int i = 0; i < expected.Length; ++i)
            {
                // Test using array
                Assert.Equal(expected[i], rows[i].GetAs<string[]>(0));

                // Test using ArrayList
                var actual = rows[i].Get(0);
                Assert.Equal(rows[i].GetAs<ArrayList>(0), actual);
                Assert.True(actual is ArrayList);
                Assert.Equal(expected[i], ((ArrayList)actual).ToArray());
            }
        }

        [Fact]
        public void TestUdfWithReturnAsArrayOfArrayType()
        {
            // Test array of arrays
            var schema = new StructType(new StructField[]
            {
                new StructField("first", new StringType()),
                new StructField("last",  new StringType())
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[] { "John", "Smith" }),
                new GenericRow(new object[] { "Jane", "Doe" })
            };

            Func<Column, Column, Column> udf =
                Udf<string, string, string[][]>((first, last) => new string[][]
                {
                    new string[] { first, last },
                    new string[] { last, first }
                });

            DataFrame df = _spark.CreateDataFrame(data, schema);
            Row[] rows = df.Select(udf(df["first"], df["last"])).Collect().ToArray();

            var expectedArr = new string[][][]
            {
                new string[][]
                {
                    new string[] { "John", "Smith" },
                    new string[] { "Smith", "John" }
                },
                new string[][]
                {
                    new string[] { "Jane", "Doe" },
                    new string[] { "Doe", "Jane" }
                }
            };

            Assert.Equal(expectedArr.Length, rows.Length);

            for (int i = 0; i < expectedArr.Length; ++i)
            {
                {
                    // Test using array
                    string[][] expected = expectedArr[i];
                    string[][] actual = rows[i].GetAs<string[][]>(0);
                    Assert.Equal(expected.Length, actual.Length);
                    for (int j = 0; j < expected.Length; ++j)
                    {
                        Assert.Equal(expected[j], actual[j]);
                    }
                }
                {
                    // Test using ArrayList
                    string[][] expected = expectedArr[i];
                    object actual = rows[i].Get(0);
                    Assert.Equal(rows[i].GetAs<ArrayList>(0), actual);
                    Assert.True(actual is ArrayList);

                    var actualArrayList = (ArrayList)actual;
                    Assert.Equal(expected.Length, actualArrayList.Count);
                    for (int j = 0; j < expected.Length; ++j)
                    {
                        Assert.True(actualArrayList[j] is ArrayList);
                        Assert.Equal(expected[j], ((ArrayList)actualArrayList[j]).ToArray());
                    }
                }
            }
        }

        [Fact]
        public void TestUdfWithArrayChain()
        {
            var schema = new StructType(new StructField[]
            {
                new StructField("first", new StringType()),
                new StructField("second", new StringType()),
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[] { "f1", "s1" }),
                new GenericRow(new object[] { "f2", "s2" }),
                new GenericRow(new object[] { "f3", "s3" }),
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);

            var expected = new string[] { "f1,s1", "f2,s2", "f3,s3" };

            // chain array
            Func<Column, Column, Column> inner = Udf<string, string, string[]>(
                (f, s) => new string[] { f, s });
            Func<Column, Column> outer = Udf<string[], string>(
                strArray => string.Join(",", strArray));
            Row[] rows =
                df.Select(outer(inner(df["first"], df["second"]))).Collect().ToArray();

            Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));

            // chaining ArrayList not supported.
        }

        /// <summary>
        /// UDF that takes in Map type.
        /// </summary>
        [Fact]
        public void TestUdfWithMapType()
        {
            DataFrame df = _df.WithColumn("NameIdsMap", Map(_df["name"], _df["ids"]));
            string[] expected = new string[] { "Michael|1", "Andy|3,5", "Justin|2,4" };

            {
                // Test Dictionary with array
                Func<Column, Column> udf = Udf<Dictionary<string, int[]>, string>(
                    dict =>
                    {
                        var sb = new StringBuilder();
                        foreach (KeyValuePair<string, int[]> kvp in dict)
                        {
                            sb.Append(kvp.Key);
                            AppendEnumerable(sb, kvp.Value);
                        }

                        return sb.ToString();
                    });

                Row[] rows = df.Select(udf(df["NameIdsMap"])).Collect().ToArray();
                Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));
            }
            {
                // Test Dictionary with ArrayList
                Func<Column, Column> udf = Udf<Dictionary<string, ArrayList>, string>(
                    dict =>
                    {
                        var sb = new StringBuilder();
                        foreach (KeyValuePair<string, ArrayList> kvp in dict)
                        {
                            sb.Append(kvp.Key);
                            AppendEnumerable(sb, kvp.Value.ToArray());
                        }

                        return sb.ToString();
                    });

                Row[] rows = df.Select(udf(df["NameIdsMap"])).Collect().ToArray();
                Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));
            }
        }

        [Fact]
        public void TestUdfWithMapOfMapType()
        {
            var schema = new StructType(new StructField[]
            {
                new StructField("first", new StringType()),
                new StructField("second", new StringType()),
                new StructField("third", new StringType()),
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[] { "f1", "s1", "t1" }),
                new GenericRow(new object[] { "f2", "s2", "t2" }),
                new GenericRow(new object[] { "f3", "s3", "t3" }),
            };

            DataFrame df = _spark
                .CreateDataFrame(data, schema)
                .WithColumn("innerMap", Map(Col("second"), Col("third")))
                .WithColumn("outerMap", Map(Col("first"), Col("innerMap")));

            string[] expected = new string[] { "f1->s1t1", "f2->s2t2", "f3->s3t3" };

            Func<Column, Column> udf = Udf<Dictionary<string, Dictionary<string, string>>, string>(
                dict =>
                {
                    var sb = new StringBuilder();
                    foreach (KeyValuePair<string, Dictionary<string, string>> kvp in dict)
                    {
                        sb.Append(kvp.Key);
                        foreach (KeyValuePair<string, string> innerKvp in kvp.Value)
                        {
                            sb.Append("->");
                            sb.Append(innerKvp.Key);
                            sb.Append(innerKvp.Value);
                        }
                    }

                    return sb.ToString();
                });

            Row[] rows = df.Select(udf(df["outerMap"])).Collect().ToArray();
            Assert.Equal(expected, rows.Select(r => r.GetAs<string>(0)));
        }

        /// <summary>
        /// UDF that returns Map type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsMapType()
        {
            var expectedDicts = new Dictionary<string, int[]>[]
            {
                new Dictionary<string, int[]> { { "Michael", new int[] { 1 } } },
                new Dictionary<string, int[]> { { "Andy", new int[] { 3, 5 } } },
                new Dictionary<string, int[]> { { "Justin", new int[] { 2, 4 } } }
            };

            Func<Column, Column, Column> udf =
                Udf<string, int[], Dictionary<string, int[]>>(
                    (name, ids) => new Dictionary<string, int[]> { { name, ids } });

            Row[] rows = _df.Select(udf(_df["name"], _df["ids"])).Collect().ToArray();

            Assert.Equal(expectedDicts.Length, rows.Length);

            // Test column as Dictionary
            Assert.Equal(expectedDicts, rows.Select(r => r.GetAs<Dictionary<string, int[]>>(0)));

            // Test column as Hashtable
            for (int i = 0; i < expectedDicts.Length; ++i)
            {
                object column = rows[i].Get(0);
                Assert.True(column is Hashtable);
                Hashtable actual = (Hashtable)column;

                Dictionary<string, int[]> expected = expectedDicts[i];
                Assert.Equal(expected.Count, actual.Count);
                foreach (KeyValuePair<string, int[]> kvp in expected)
                {
                    Assert.True(actual.ContainsKey(kvp.Key));
                    Assert.Equal(kvp.Value, actual[kvp.Key]);
                }
            }
        }

        /// <summary>
        /// UDF that takes in Row type.
        /// </summary>
        [Fact]
        public void TestUdfWithRowType()
        {
            // Single Row.
            {
                Func<Column, Column> udf = Udf<Row, string>(
                    (row) => row.GetAs<string>("city"));

                Row[] rows = _df.Select(udf(_df["info1"])).Collect().ToArray();
                Assert.Equal(3, rows.Length);

                var expected = new string[] { "Burdwan", "Los Angeles", "Seattle" };
                string[] actual = rows.Select(x => x[0].ToString()).ToArray();
                Assert.Equal(expected, actual);
            }

            // Multiple Rows.
            {
                Func<Column, Column, Column, Column> udf = Udf<Row, Row, string, string>(
                    (row1, row2, str) =>
                    {
                        string city = row1.GetAs<string>("city");
                        string state = row2.GetAs<string>("state");
                        return $"{str}:{city},{state}";
                    });

                Row[] rows = _df
                    .Select(udf(_df["info1"], _df["info2"], _df["name"]))
                    .Collect()
                    .ToArray();
                Assert.Equal(3, rows.Length);

                var expected = new string[] {
                    "Michael:Burdwan,Paschimbanga",
                    "Andy:Los Angeles,California",
                    "Justin:Seattle,Washington" };
                string[] actual = rows.Select(x => x[0].ToString()).ToArray();
                Assert.Equal(expected, actual);
            }

            // Nested Rows.
            {
                Func<Column, Column> udf = Udf<Row, string>(
                    (row) =>
                    {
                        Row outerCol = row.GetAs<Row>("company");
                        return outerCol.GetAs<string>("job");
                    });

                Row[] rows = _df.Select(udf(_df["info3"])).Collect().ToArray();
                Assert.Equal(3, rows.Length);

                var expected = new string[] { "Developer", "Developer", "Developer" };
                string[] actual = rows.Select(x => x[0].ToString()).ToArray();
                Assert.Equal(expected, actual);
            }
        }

        /// <summary>
        /// UDF that returns Row type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsRowType()
        {
            // Test UDF that returns a Row object with a single column.
            {
                var schema = new StructType(new[]
                {
                    new StructField("col1", new IntegerType()),
                    new StructField("col2", new StringType())
                });
                Func<Column, Column> udf = Udf<string>(
                    str => new GenericRow(new object[] { 1, "abc" }), schema);

                Row[] rows = _df.Select(udf(_df["name"]).As("col")).Collect().ToArray();
                Assert.Equal(3, rows.Length);
                foreach (Row row in rows)
                {
                    Assert.Equal(1, row.Size());
                    Row outerCol = row.GetAs<Row>("col");
                    Assert.Equal(2, outerCol.Size());
                    Assert.Equal(1, outerCol.GetAs<int>("col1"));
                    Assert.Equal("abc", outerCol.GetAs<string>("col2"));
                }
            }

            // Test UDF that returns a Row object with multiple columns.
            {
                var schema = new StructType(new[]
                {
                    new StructField("col1", new IntegerType())
                });
                Func<Column, Column> udf = Udf<string>(
                    str => new GenericRow(new object[] { 111 }), schema);

                Column nameCol = _df["name"];
                Row[] rows = _df.Select(udf(nameCol).As("col"), nameCol).Collect().ToArray();
                Assert.Equal(3, rows.Length);

                foreach (Row row in rows)
                {
                    Assert.Equal(2, row.Size());
                    Row col1 = row.GetAs<Row>("col");
                    Assert.Equal(1, col1.Size());
                    Assert.Equal(111, col1.GetAs<int>("col1"));

                    string col2 = row.GetAs<string>("name");
                    Assert.NotEmpty(col2);
                }
            }

            // Test UDF that returns a nested Row object.
            {
                var subSchema1 = new StructType(new[]
                {
                    new StructField("col1", new IntegerType()),
                });
                var subSchema2 = new StructType(new[]
                {
                    new StructField("col1", new StringType()),
                    new StructField("col2", subSchema1),
                });
                var schema = new StructType(new[]
                {
                    new StructField("col1", new IntegerType()),
                    new StructField("col2", subSchema1),
                    new StructField("col3", subSchema2)
                });

                Func<Column, Column> udf = Udf<string>(
                    str => new GenericRow(
                        new object[]
                        {
                            1,
                            new GenericRow(new object[] { 1 }),
                            new GenericRow(new object[]
                                {
                                    "abc",
                                    new GenericRow(new object[] { 10 })
                                })
                        }),
                        schema);

                Row[] rows = _df.Select(udf(_df["name"]).As("col")).Collect().ToArray();
                Assert.Equal(3, rows.Length);

                foreach (Row row in rows)
                {
                    Assert.Equal(1, row.Size());
                    Row outerCol = row.GetAs<Row>("col");
                    Assert.Equal(3, outerCol.Size());
                    Assert.Equal(1, outerCol.GetAs<int>("col1"));
                    Assert.Equal(
                        new Row(new object[] { 1 }, subSchema1),
                        outerCol.GetAs<Row>("col2"));
                    Assert.Equal(
                        new Row(
                            new object[] { "abc", new Row(new object[] { 10 }, subSchema1) },
                            subSchema2),
                        outerCol.GetAs<Row>("col3"));
                }
            }

            // Chained UDFs.
            {
                var schema = new StructType(new[]
                {
                    new StructField("col1", new IntegerType()),
                    new StructField("col2", new StringType())
                });
                Func<Column, Column> udf1 = Udf<string>(
                    str => new GenericRow(new object[] { 1, str }), schema);

                Func<Column, Column> udf2 = Udf<Row, string>(
                    row => row.GetAs<string>(1));

                Row[] rows = _df.Select(udf2(udf1(_df["name"]))).Collect().ToArray();
                Assert.Equal(3, rows.Length);

                var expected = new[] { "Michael", "Andy", "Justin" };
                for (int i = 0; i < rows.Length; ++i)
                {
                    Assert.Equal(1, rows[i].Size());
                    Assert.Equal(expected[i], rows[i].GetAs<string>(0));
                }
            }
        }

        /// <summary>
        /// UDF Registration with UDF that returns Row type.
        /// </summary>
        [Fact]
        public void TestUdfRegistrationWithReturnAsRowType()
        {
            // Test UDF that returns a Row object with a single column.
            {
                var schema = new StructType(new[]
                {
                    new StructField("col1", new IntegerType()),
                    new StructField("col2", new StringType())
                });

                _df.CreateOrReplaceTempView("people");

                _spark.Udf().Register<string>(
                    "udf1",
                    str => new GenericRow(new object[] { 1, "abc" }),
                    schema);

                Row[] rows =
                    _spark.Sql("SELECT udf1(name) AS col FROM people")
                        .Collect()
                        .ToArray();
                Assert.Equal(3, rows.Length);
                foreach (Row row in rows)
                {
                    Assert.Equal(1, row.Size());
                    Row outerCol = row.GetAs<Row>("col");
                    Assert.Equal(2, outerCol.Size());
                    Assert.Equal(1, outerCol.GetAs<int>("col1"));
                    Assert.Equal("abc", outerCol.GetAs<string>("col2"));
                }
            }

            // Test UDF that returns a Row object with multiple columns.
            {
                var schema = new StructType(new[]
                {
                    new StructField("col1", new IntegerType())
                });

                _df.CreateOrReplaceTempView("people");

                _spark.Udf().Register<string>(
                    "udf2",
                    str => new GenericRow(new object[] { 111 }),
                    schema);

                Row[] rows =
                    _spark.Sql("SELECT udf2(name) AS col, name FROM people")
                        .Collect()
                        .ToArray();
                Assert.Equal(3, rows.Length);

                foreach (Row row in rows)
                {
                    Assert.Equal(2, row.Size());
                    Row col1 = row.GetAs<Row>("col");
                    Assert.Equal(1, col1.Size());
                    Assert.Equal(111, col1.GetAs<int>("col1"));

                    string col2 = row.GetAs<string>("name");
                    Assert.NotEmpty(col2);
                }
            }

            // Test UDF that returns a nested Row object.
            {
                var subSchema1 = new StructType(new[]
                {
                    new StructField("col1", new IntegerType()),
                });
                var subSchema2 = new StructType(new[]
                {
                    new StructField("col1", new StringType()),
                    new StructField("col2", subSchema1),
                });
                var schema = new StructType(new[]
                {
                    new StructField("col1", new IntegerType()),
                    new StructField("col2", subSchema1),
                    new StructField("col3", subSchema2)
                });

                _df.CreateOrReplaceTempView("people");

                _spark.Udf().Register<string>(
                    "udf3",
                    str => new GenericRow(
                        new object[]
                        {
                            1,
                            new GenericRow(new object[] { 1 }),
                            new GenericRow(new object[]
                                {
                                    "abc",
                                    new GenericRow(new object[] { 10 })
                                })
                        }),
                    schema);

                Row[] rows =
                    _spark.Sql("SELECT udf3(name) AS col FROM people")
                        .Collect()
                        .ToArray();
                Assert.Equal(3, rows.Length);

                foreach (Row row in rows)
                {
                    Assert.Equal(1, row.Size());
                    Row outerCol = row.GetAs<Row>("col");
                    Assert.Equal(3, outerCol.Size());
                    Assert.Equal(1, outerCol.GetAs<int>("col1"));
                    Assert.Equal(
                        new Row(new object[] { 1 }, subSchema1),
                        outerCol.GetAs<Row>("col2"));
                    Assert.Equal(
                        new Row(
                            new object[] { "abc", new Row(new object[] { 10 }, subSchema1) },
                            subSchema2),
                        outerCol.GetAs<Row>("col3"));
                }
            }

            // Chained UDFs.
            {
                var schema = new StructType(new[]
                {
                    new StructField("col1", new IntegerType()),
                    new StructField("col2", new StringType())
                });

                _df.CreateOrReplaceTempView("people");

                _spark.Udf().Register<string>(
                    "udf4",
                    str => new GenericRow(new object[] { 1, str }),
                    schema);

                _spark.Udf().Register<Row, string>(
                    "udf5",
                    row => row.GetAs<string>(1));

                Row[] rows =
                    _spark.Sql("SELECT udf5(udf4(name)) FROM people")
                        .Collect()
                        .ToArray();
                Assert.Equal(3, rows.Length);

                var expected = new string[] { "Michael", "Andy", "Justin" };
                for (int i = 0; i < rows.Length; ++i)
                {
                    Assert.Equal(1, rows[i].Size());
                    Assert.Equal(expected[i], rows[i].GetAs<string>(0));
                }
            }
        }

        private static string AppendEnumerable<T>(string s, IEnumerable<T> enumerable) =>
            s + "|" + string.Join(",", enumerable);

        private static void AppendEnumerable<T>(StringBuilder sb, IEnumerable<T> enumerable) =>
            sb.Append("|" + string.Join(",", enumerable));
    }
}
