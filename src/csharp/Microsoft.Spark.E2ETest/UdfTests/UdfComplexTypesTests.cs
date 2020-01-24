// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
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
        public void TestUdfWithArrayType()
        {
            // UDF with ArrayType throws a following exception:
            // [] [] [Error] [TaskRunner] [0] ProcessStream() failed with exception: System.InvalidCastException: Unable to cast object of type 'System.Collections.ArrayList' to type 'System.Int32[]'.
            //  at Microsoft.Spark.Sql.PicklingUdfWrapper`2.Execute(Int32 splitIndex, Object[] input, Int32[] argOffsets) in Microsoft.Spark\Sql\PicklingUdfWrapper.cs:line 44
            //  at Microsoft.Spark.Worker.Command.PicklingSqlCommandExecutor.SingleCommandRunner.Run(Int32 splitId, Object input) in Microsoft.Spark.Worker\Command\SqlCommandExecutor.cs:line 239
            //  at Microsoft.Spark.Worker.Command.PicklingSqlCommandExecutor.ExecuteCore(Stream inputStream, Stream outputStream, SqlCommand[] commands) in Microsoft.Spark.Worker\Command\SqlCommandExecutor.cs:line 139
            Func<Column, Column> udf = Udf<int[], string>(array => string.Join(',', array));
            Assert.Throws<Exception>(() => _df.Select(udf(_df["ids"])).Show());

            // Currently, there is a workaround to support ArrayType using ArrayList.
            Func<Column, Column> workingUdf = Udf<ArrayList, string>(
                array => string.Join(',', array.ToArray()));

            Row[] rows = _df.Select(workingUdf(_df["ids"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);

            var expected = new[] { "1", "3,5", "2,4" };
            string[] rowsToArray = rows.Select(x => x[0].ToString()).ToArray();
            Assert.Equal(expected, rowsToArray);
        }

        /// <summary>
        /// UDF that returns Array type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsArrayType()
        {
            // UDF with return as ArrayType throws a following exception:
            // Unhandled Exception: System.Reflection.TargetInvocationException: Exception has been thrown by the target of an invocation. 
            // ---> System.NotImplementedException: The method or operation is not implemented.
            // at Microsoft.Spark.Sql.Row.Convert() in Microsoft.Spark\Sql\Row.cs:line 169
            // at Microsoft.Spark.Sql.Row..ctor(Object[] values, StructType schema) in Microsoft.Spark\Sql\Row.cs:line 34
            // at Microsoft.Spark.Sql.RowConstructor.GetRow() in Microsoft.Spark\Sql\RowConstructor.cs:line 113
            // at Microsoft.Spark.Sql.RowCollector.Collect(ISocketWrapper socket) + MoveNext() in Microsoft.Spark\Sql\RowCollector.cs:line 36
            // at Microsoft.Spark.Sql.DataFrame.GetRows(String funcName) + MoveNext() in Microsoft.Spark\Sql\DataFrame.cs:line 891
            Func<Column, Column> udf = Udf<string, string[]>(
                str => new string[] { str, str + str });
            Assert.Throws<NotImplementedException>(
                () => _df.Select(udf(_df["name"])).Collect().ToArray());

            // Show() works here. See the example below.
            _df.Select(udf(_df["name"])).Show();
        }

        /// <summary>
        /// UDF that takes in Map type.
        /// </summary>
        [Fact]
        public void TestUdfWithMapType()
        {
            // UDF with MapType throws a following exception:
            // [] [] [Error] [TaskRunner] [0] ProcessStream() failed with exception: System.InvalidCastException: Unable to cast object of type 'System.Collections.Hashtable' to type 'System.Collections.Generic.IDictionary`2[System.String,System.Int32[]]'.
            // at Microsoft.Spark.Sql.PicklingUdfWrapper`2.Execute(Int32 splitIndex, Object[] input, Int32[] argOffsets) in Microsoft.Spark\Sql\PicklingUdfWrapper.cs:line 44
            // at Microsoft.Spark.Worker.Command.PicklingSqlCommandExecutor.SingleCommandRunner.Run(Int32 splitId, Object input) in Microsoft.Spark.Worker\Command\SqlCommandExecutor.cs:line 239
            // at Microsoft.Spark.Worker.Command.PicklingSqlCommandExecutor.ExecuteCore(Stream inputStream, Stream outputStream, SqlCommand[] commands) in Microsoft.Spark.Worker\Command\SqlCommandExecutor.cs:line 139
            Func<Column, Column> udf = Udf<IDictionary<string, int[]>, string>(
                dict => dict.Count.ToString());

            DataFrame df = _df.WithColumn("NameIdsMap", Map(_df["name"], _df["ids"]));
            Assert.Throws<Exception>(() => df.Select(udf(df["NameIdsMap"])).Show());

            // Currently, there is a workaround to support MapType using Hashtable.
            Func<Column, Column> workingUdf = Udf<Hashtable, string>(
                dict => dict.Count.ToString());

            Row[] rows = df.Select(workingUdf(df["NameIdsMap"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);

            var expected = new[] { "1", "1", "1" };
            string[] actual = rows.Select(x => x[0].ToString()).ToArray();
            Assert.Equal(expected, actual);
        }

        /// <summary>
        /// UDF that returns Map type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsMapType()
        {
            // UDF with return as MapType throws a following exception:
            // Unhandled Exception: System.Reflection.TargetInvocationException: Exception has been thrown by the target of an invocation. 
            // ---> System.NotImplementedException: The method or operation is not implemented.
            // System.NotImplementedException: The method or operation is not implemented.
            // at Microsoft.Spark.Sql.Row.Convert() in Microsoft.Spark\Sql\Row.cs:line 169
            // at Microsoft.Spark.Sql.Row..ctor(Object[] values, StructType schema) in Microsoft.Spark\Sql\Row.cs:line 34
            // at Microsoft.Spark.Sql.RowConstructor.GetRow() in MicrTosoft.Spark\Sql\RowConstructor.cs:line 113
            // at Microsoft.Spark.Sql.RowCollector.Collect(ISocketWrapper socket) + MoveNext() in Microsoft.Spark\Sql\RowCollector.cs:line 36
            // at Microsoft.Spark.Sql.DataFrame.GetRows(String funcName) + MoveNext() in Microsoft.Spark\Sql\DataFrame.cs:line 891
            Func<Column, Column> udf = Udf<string, IDictionary<string, string>>(
                str => new Dictionary<string, string> { { str, str } });
            Assert.Throws<NotImplementedException>(
                () => _df.Select(udf(_df["name"])).Collect().ToArray());

            // Show() works here. See the example below.
            _df.Select(udf(_df["name"])).Show();
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
            string[] actual = rows.Select(x => x[0].ToString()).ToArray();
            Assert.Equal(expected, actual);
        }

        /// <summary>
        /// UDF that returns Row type.
        /// </summary>
        [Fact]
        public void TestUdfWithReturnAsRowType()
        {
            // Single GenericRow
            {
                var schema = new StructType(new[]
                {
                    new StructField("col1", new IntegerType()),
                    new StructField("col2", new StringType())
                });
                Func<Column, Column> udf = Udf<string>(
                    str => new GenericRow(new object[] { 1, "abc" }), schema);

                Row[] rows = _df.Select(udf(_df["name"])).Collect().ToArray();
                Assert.Equal(3, rows.Length);

                foreach (Row row in rows)
                {
                    Assert.Equal(2, row.Size());
                    Assert.Equal(1, row.GetAs<int>("col1"));
                    Assert.Equal("abc", row.GetAs<string>("col2"));
                }
            }

            // Nested GenericRow
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

                Row[] rows = _df.Select(udf(_df["name"])).Collect().ToArray();
                Assert.Equal(3, rows.Length);

                foreach (Row row in rows)
                {
                    Assert.Equal(3, row.Size());
                    Assert.Equal(1, row.GetAs<int>("col1"));
                    Assert.Equal(
                        new Row(new object[] { 1 }, subSchema1),
                        row.GetAs<Row>("col2"));
                    Assert.Equal(
                        new Row(
                            new object[] { "abc", new Row(new object[] { 10 }, subSchema1) },
                            subSchema2),
                        row.GetAs<Row>("col3"));
                }
            }
        }
    }
}
