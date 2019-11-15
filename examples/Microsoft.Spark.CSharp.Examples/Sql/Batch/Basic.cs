// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Examples.Sql.Batch
{
    /// <summary>
    /// A simple example demonstrating basic Spark SQL features.
    /// </summary>
    internal sealed class Basic : IExample
    {
        public void Run(string[] args)
        {
            SparkSession spark = SparkSession
                .Builder()
                .AppName(".NET Spark SQL basic example")
                .Config("spark.some.config.option", "some-value")
                .GetOrCreate();

            // Need to explicitly specify the schema since pickling vs. arrow formatting
            // will return different types. Pickling will turn longs into ints if the values fit.                            

            DataFrame df = spark.Read().Schema("age INT, name STRING").Json(args[0]);

            Spark.Sql.Types.StructType schema = df.Schema();
            Console.WriteLine(schema.SimpleString);

            IEnumerable<Row> rows = df.Collect();
            foreach (Row row in rows)
            {
                Console.WriteLine(row);
            }

            df.Show();

            df.PrintSchema();

            df.Select("name", "age", "age", "name").Show();

            df.Select(df["name"], df["age"] + 1).Show();

            df.Filter(df["age"] > 21).Show();

            df.GroupBy("age")
                .Agg(Avg(df["age"]), Avg(df["age"]), CountDistinct(df["age"], df["age"]))
                .Show();

            df.CreateOrReplaceTempView("people");

            // Registering Udf for SQL expression.
            DataFrame sqlDf = spark.Sql("SELECT * FROM people");
            sqlDf.Show();

            spark.Udf().Register<int?, string, string>(
                "my_udf",
                (age, name) => name + " with " + ((age.HasValue) ? age.Value.ToString() : "null"));

            sqlDf = spark.Sql("SELECT my_udf(*) FROM people");
            sqlDf.Show();

            // Using UDF via data frames.
            Func<Column, Column, Column> addition = Udf<int?, string, string>(
                (age, name) => name + " is " + (age.HasValue ? age.Value + 10 : 0));
            df.Select(addition(df["age"], df["name"])).Show();

            // Chaining example:
            Func<Column, Column> addition2 = Udf<string, string>(str => $"hello {str}!");
            df.Select(addition2(addition(df["age"], df["name"]))).Show();

            // Multiple UDF example:
            df.Select(addition(df["age"], df["name"]), addition2(df["name"])).Show();

            // UDF return type as array.
            Func<Column, Column> udfArray =
                Udf<string, string[]>((str) => new string[] { str, str + str });
            df.Select(Explode(udfArray(df["name"]))).Show();

            // UDF return type as map.
            Func<Column, Column> udfMap =
                Udf<string, IDictionary<string, string[]>>(
                    (str) => new Dictionary<string, string[]> { { str, new[] { str, str } } });
            df.Select(udfMap(df["name"]).As("UdfMap")).Show(truncate: 50);

            // Joins.
            DataFrame joinedDf = df.Join(df, "name");
            joinedDf.Show();

            DataFrame joinedDf2 = df.Join(df, new[] { "name", "age" });
            joinedDf2.Show();

            DataFrame joinedDf3 = df.Join(df, df["name"] == df["name"], "outer");
            joinedDf3.Show();

            // CreateDataFrame returning a dataframe given a list of Rows and a StructType schema

            var structFields = new List<StructField>()
            {
                new StructField("Name", new StringType()),
                new StructField("Role", new StringType()),
                new StructField("Id", new IntegerType()),
                new StructField("Age", new LongType()),
            };

            var inputSchema = new StructType(structFields);

            var row1 = new Row(new object[] { "Alice", "SWE", 1, 30L }, inputSchema);
            var row2 = new Row(new object[] { "Bob", "PM", 2, 32L }, inputSchema);

            List<Row> data = new List<Row>();
            data.Add(row1);
            data.Add(row2);

            DataFrame df2 = spark.CreateDataFrame(data, inputSchema);
            df2.Show();

            spark.Stop();
        }
    }
}
