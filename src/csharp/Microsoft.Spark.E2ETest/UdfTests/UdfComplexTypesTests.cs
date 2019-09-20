// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
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
                .Json(Path.Combine($"{TestEnvironment.ResourceDirectory}people.json"));
        }

        /// <summary>
        /// UDF that takes in Array type.
        /// </summary>
        [Fact]
        public void TestUdfWithArrayType()
        {
            // UDF with array throws a following exception:
            // [] [] [Error] [TaskRunner] [0] ProcessStream() failed with exception: System.InvalidCastException: Unable to cast object of type 'System.Collections.ArrayList' to type 'System.Int32[]'.
            //  at Microsoft.Spark.Sql.PicklingUdfWrapper`2.Execute(Int32 splitIndex, Object[] input, Int32[] argOffsets) in Microsoft.Spark\Sql\PicklingUdfWrapper.cs:line 44
            //  at Microsoft.Spark.Worker.Command.PicklingSqlCommandExecutor.SingleCommandRunner.Run(Int32 splitId, Object input) in Microsoft.Spark.Worker\Command\SqlCommandExecutor.cs:line 239
            //  at Microsoft.Spark.Worker.Command.PicklingSqlCommandExecutor.ExecuteCore(Stream inputStream, Stream outputStream, SqlCommand[] commands) in Microsoft.Spark.Worker\Command\SqlCommandExecutor.cs:line 139
            Func<Column, Column> udfInt = Udf<int[], string>(array => string.Join(',', array));
            Assert.Throws<Exception>(() => _df.Select(udfInt(_df["ages"])).Show());

            // Currently, there is a workaround to support array type using ArrayList. See the example below.
            Func<Column, Column> workingUdf = Udf<ArrayList, string>(array => string.Join(',', array.ToArray()));

            Row[] rows = _df.Select(workingUdf(_df["ages"])).Collect().ToArray();
            Assert.Equal(3, rows.Length);
            
            var expected = new[] { "19", "19,30", "30,40" };
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
        public void TestUdfWithReturnTypeAsArray()
        {
            // System.NotImplementedException: The method or operation is not implemented.
            // at Microsoft.Spark.Sql.Row.Convert() in Microsoft.Spark\Sql\Row.cs:line 169
            // at Microsoft.Spark.Sql.Row..ctor(Object[] values, StructType schema) in Microsoft.Spark\Sql\Row.cs:line 34
            // at Microsoft.Spark.Sql.RowConstructor.GetRow() in Microsoft.Spark\Sql\RowConstructor.cs:line 113
            // at Microsoft.Spark.Sql.RowCollector.Collect(ISocketWrapper socket) + MoveNext() in Microsoft.Spark\Sql\RowCollector.cs:line 36
            // at Microsoft.Spark.Sql.DataFrame.GetRows(String funcName) + MoveNext() in Microsoft.Spark\Sql\DataFrame.cs:line 891
            Func<Column, Column> udf = Udf<string, string[]>(
                str => new string[] { str, str + str });
            Assert.Throws<NotImplementedException>(() => _df.Select(udf(_df["name"])).Collect().ToArray());
        }

        /// <summary>
        /// UDF that takes in Map type.
        /// </summary>
        [Fact]
        public void TestUdfWithMapType()
        {
            // System.NotImplementedException: The method or operation is not implemented.
            // at Microsoft.Spark.Sql.Row.Convert() in Microsoft.Spark\Sql\Row.cs:line 169
            // at Microsoft.Spark.Sql.Row..ctor(Object[] values, StructType schema) in Microsoft.Spark\Sql\Row.cs:line 34
            // at Microsoft.Spark.Sql.RowConstructor.GetRow() in Microsoft.Spark\Sql\RowConstructor.cs:line 113
            // at Microsoft.Spark.Sql.RowCollector.Collect(ISocketWrapper socket) + MoveNext() in Microsoft.Spark\Sql\RowCollector.cs:line 36
            // at Microsoft.Spark.Sql.DataFrame.GetRows(String funcName) + MoveNext() in Microsoft.Spark\Sql\DataFrame.cs:line 891
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
            // System.NotImplementedException: The method or operation is not implemented.
            // at Microsoft.Spark.Sql.Row.Convert() in Microsoft.Spark\Sql\Row.cs:line 169
            // at Microsoft.Spark.Sql.Row..ctor(Object[] values, StructType schema) in Microsoft.Spark\Sql\Row.cs:line 34
            // at Microsoft.Spark.Sql.RowConstructor.GetRow() in MicrTosoft.Spark\Sql\RowConstructor.cs:line 113
            // at Microsoft.Spark.Sql.RowCollector.Collect(ISocketWrapper socket) + MoveNext() in Microsoft.Spark\Sql\RowCollector.cs:line 36
            // at Microsoft.Spark.Sql.DataFrame.GetRows(String funcName) + MoveNext() in Microsoft.Spark\Sql\DataFrame.cs:line 891
            Func<Column, Column> udf = Udf<string, IDictionary<string, string>>(
                str => new Dictionary<string, string> { { str, str } });
            Assert.Throws<NotImplementedException>(() => _df.Select(udf(_df["name"])).Collect().ToArray());
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
