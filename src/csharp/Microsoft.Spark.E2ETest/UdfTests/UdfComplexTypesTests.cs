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

            // Currently, there is a workaround to support ArrayType using ArrayList. See the example below.
            Func<Column, Column> workingUdf = Udf<ArrayList, string>(array => string.Join(',', array.ToArray()));

            Row[] rows = _df.Select(workingUdf(_df["ids"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);
            
            var expected = new[] { "1", "3,5","2,4" };
            for (int i = 0; i < rows.Length; ++i)
            {
                Row row = rows[i];
                Assert.Equal(1, row.Size());
                Assert.Equal(expected[i], row.GetAs<string>(0));
            }
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
            Assert.Throws<NotImplementedException>(() => _df.Select(udf(_df["name"])).Collect().ToArray());

            //Show() works here. See the example below.
            _df.Select(udf(_df["name"])).Show();
        }

        /// <summary>
        /// UDF that takes in Map type.
        /// </summary>
        [Fact]
        public void TestUdfWithMapType()
        {
            // UDF with MapType throws a following exception:
            // [] [] [Error] [TaskRunner] [0] ProcessStream() failed with exception: System.InvalidCastException: Unable to cast object of type 'System.Collections.Hashtable' to type 'System.Collections.Generic.IDictionary`2[System.String,System.String]'.
            // at Microsoft.Spark.Sql.PicklingUdfWrapper`2.Execute(Int32 splitIndex, Object[] input, Int32[] argOffsets) in Microsoft.Spark\Sql\PicklingUdfWrapper.cs:line 44
            // at Microsoft.Spark.Worker.Command.PicklingSqlCommandExecutor.SingleCommandRunner.Run(Int32 splitId, Object input) in Microsoft.Spark.Worker\Command\SqlCommandExecutor.cs:line 239
            // at Microsoft.Spark.Worker.Command.PicklingSqlCommandExecutor.ExecuteCore(Stream inputStream, Stream outputStream, SqlCommand[] commands) in Microsoft.Spark.Worker\Command\SqlCommandExecutor.cs:line 139
            Func<Column, Column> udf = Udf<IDictionary<string, string>, string>(
                    dict => dict.Count.ToString());

            DataFrame df = _df.WithColumn("tempName", Map(_df["name"], _df["name"]));
            Assert.Throws<Exception>(() => df.Select(udf(df["tempName"])).Show());

            // Currently, there is a workaround to support MapType using Hashtable. See the example below.
            Func<Column, Column> workingUdf = Udf<Hashtable, string>(
                dict => dict.Count.ToString());

            Row[] rows = df.Select(workingUdf(df["tempName"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);

            var expected = new[] { "1", "1", "1" };
            for (int i = 0; i < rows.Length; ++i)
            {
                Row row = rows[i];
                Assert.Equal(1, row.Size());
                Assert.Equal(expected[i], row.GetAs<string>(0));
            }
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
            Assert.Throws<NotImplementedException>(() => _df.Select(udf(_df["name"])).Collect().ToArray());

            //Show() works here. See the example below.
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
        public void TestUdfWithReturnAsRowType()
        {
            // UDF with return as RowType throws a following exception:
            // Unhandled Exception: System.Reflection.TargetInvocationException: Exception has been thrown by the target of an invocation. 
            // --->System.ArgumentException: System.Object is not supported.
            // at Microsoft.Spark.Utils.UdfUtils.GetReturnType(Type type) in Microsoft.Spark\Utils\UdfUtils.cs:line 142
            // at Microsoft.Spark.Utils.UdfUtils.GetReturnType(Type type) in Microsoft.Spark\Utils\UdfUtils.cs:line 136
            // at Microsoft.Spark.Sql.Functions.CreateUdf[TResult](String name, Delegate execute, PythonEvalType evalType) in Microsoft.Spark\Sql\Functions.cs:line 4053
            // at Microsoft.Spark.Sql.Functions.CreateUdf[TResult](String name, Delegate execute) in Microsoft.Spark\Sql\Functions.cs:line 4040
            // at Microsoft.Spark.Sql.Functions.Udf[T, TResult](Func`2 udf) in Microsoft.Spark\Sql\Functions.cs:line 3607
            Assert.Throws<ArgumentException>(() => Udf<string, Row>(
                (str) =>
                {
                    var structFields = new List<StructField>()
                    {
                        new StructField("name", new StringType()),
                    };
                    var schema = new StructType(structFields);
                    var row = new Row(new object[] { str }, schema);
                    return row;
                }));
        }
    }
}
