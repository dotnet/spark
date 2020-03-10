// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using System.Linq;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;
using Xunit;


namespace Microsoft.Spark.Examples.Sql.Batch
{
    [Serializable]
    public class TestBroadcastVariable
    {
        public int IntValue { get; private set; }
        public string StringValue { get; private set; }

        public TestBroadcastVariable(int intVal, string stringVal)
        {
            IntValue = intVal;
            StringValue = stringVal;
        }
    }

    /// <summary>
    /// A simple example demonstrating basic Spark SQL features.
    /// </summary>
    internal sealed class Basic : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 1)
            {
                Console.Error.WriteLine(
                    "Usage: Basic <path to SPARK_HOME/examples/src/main/resources/people.json>");
                Environment.Exit(1);
            }

            SparkSession spark = SparkSession
                .Builder()
                .AppName(".NET Spark SQL basic example")
                .Config("spark.some.config.option", "some-value")
                .GetOrCreate();

            //// Need to explicitly specify the schema since pickling vs. arrow formatting
            //// will return different types. Pickling will turn longs into ints if the values fit.
            //// Same as the "age INT, name STRING" DDL-format string.
            //var inputSchema = new StructType(new[]
            //{
            //    new StructField("age", new IntegerType()),
            //    new StructField("name", new StringType())
            //});
            //DataFrame df = spark.Read().Schema(inputSchema).Json(args[0]);

            //Spark.Sql.Types.StructType schema = df.Schema();
            //Console.WriteLine(schema.SimpleString);

            //IEnumerable<Row> rows = df.Collect();
            //foreach (Row row in rows)
            //{
            //    Console.WriteLine(row);
            //}

            //df.Show();

            //df.PrintSchema();

            //df.Select("name", "age", "age", "name").Show();

            //df.Select(df["name"], df["age"] + 1).Show();

            //df.Filter(df["age"] > 21).Show();

            //df.GroupBy("age")
            //    .Agg(Avg(df["age"]), Avg(df["age"]), CountDistinct(df["age"], df["age"]))
            //    .Show();

            //df.CreateOrReplaceTempView("people");

            //// Registering Udf for SQL expression.
            //DataFrame sqlDf = spark.Sql("SELECT * FROM people");
            //sqlDf.Show();

            //spark.Udf().Register<int?, string, string>(
            //    "my_udf",
            //    (age, name) => name + " with " + ((age.HasValue) ? age.Value.ToString() : "null"));

            //sqlDf = spark.Sql("SELECT my_udf(*) FROM people");
            //sqlDf.Show();

            //// Using UDF via data frames.
            //Func<Column, Column, Column> addition = Udf<int?, string, string>(
            //    (age, name) => name + " is " + (age.HasValue ? age.Value + 10 : 0));
            //df.Select(addition(df["age"], df["name"])).Show();

            //// Chaining example:
            //Func<Column, Column> addition2 = Udf<string, string>(str => $"hello {str}!");
            //df.Select(addition2(addition(df["age"], df["name"]))).Show();

            //// Multiple UDF example:
            //df.Select(addition(df["age"], df["name"]), addition2(df["name"])).Show();

            //// UDF return type as array.
            //Func<Column, Column> udfArray =
            //    Udf<string, string[]>((str) => new[] { str, str + str });
            //df.Select(Explode(udfArray(df["name"]))).Show();

            //// UDF return type as map.
            //Func<Column, Column> udfMap =
            //    Udf<string, IDictionary<string, string[]>>(
            //        (str) => new Dictionary<string, string[]> { { str, new[] { str, str } } });
            //df.Select(udfMap(df["name"]).As("UdfMap")).Show(truncate: 50);

            //// Joins.
            //DataFrame joinedDf = df.Join(df, "name");
            //joinedDf.Show();

            //DataFrame joinedDf2 = df.Join(df, new[] { "name", "age" });
            //joinedDf2.Show();

            //DataFrame joinedDf3 = df.Join(df, df["name"] == df["name"], "outer");
            //joinedDf3.Show();

            var obj1 = new TestBroadcastVariable(1, "first");
            var obj2 = new TestBroadcastVariable(2, "second");
            Broadcast<TestBroadcastVariable> bc1 = spark.SparkContext.Broadcast(obj1);
            //Broadcast<TestBroadcastVariable> bc2 = spark.SparkContext.Broadcast(obj2);

            DataFrame _df = spark.CreateDataFrame(new[] { "hello", "world" });

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc1.Value().StringValue}, {bc1.Value().IntValue}");
            //Func<Column, Column> udf = Udf<string, string>(
            //    str => $"");
            string[] expected = new[] { "hello first, 1", "world first, 1" };

            Row[] actualRows = _df.Select(udf(_df["_1"])).Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);
            bc1.Destroy();
            Assert.Throws<Exception>(() => _df.Select(udf(_df["_1"])).Collect().ToArray());
            try
            {
                Row[] testRows = _df.Select(udf(_df["_1"])).Collect().ToArray();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Attempted to use broadcast after being destroyed: {e.Message}");
            }
            Func<Column, Column> udf2 = Udf<string, string>(
                str => $"{str} {bc1.Value().StringValue}, {bc1.Value().IntValue}");
            //Assert.Throws<Exception>(() => _df.Select(udf2(_df["_1"])).Collect().ToArray());
            try
            {
                Row[] testRows = _df.Select(udf2(_df["_1"])).Collect().ToArray();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Attempted to use broadcast after being destroyed: {e.Message}");
            }
            //Func<Column, Column> udf3 = Udf<string, string>(
            //    str => $"{str} {bc2.Value().StringValue}, {bc2.Value().IntValue}");
            //string[] expected2 = new[] { "hello second, 2", "world second, 2" };
            //Row[] actualRows2 = _df.Select(udf3(_df["_1"])).Collect().ToArray();
            //string[] actual2 = actualRows2.Select(s => s[0].ToString()).ToArray();
            //Assert.Equal(expected2, actual2);

            spark.Stop();
        }
    }
}
