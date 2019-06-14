// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.Examples.Sql
{
    /// <summary>
    /// A simple example demonstrating basic Spark SQL features.
    /// /// </summary>
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

            // Grouped Map Vector UDF
            // able to return different shapes and record lengths
            Func<RecordBatch, RecordBatch> countChars = batch => CountCharacters(batch, "age", "name");

            df.GroupBy("age")
                .Apply(
                    new Spark.Sql.Types.StructType(new[]
                    {
                        new StructField("age", new IntegerType()),
                        new StructField("nameCharCount", new IntegerType())
                    }),
                    countChars)
                .Show();

            spark.Stop();
        }

        private static RecordBatch CountCharacters(
            RecordBatch records,
            string groupFieldName,
            string stringFieldName)
        {
            var stringFieldIndex = records.Schema.GetFieldIndex(stringFieldName);
            StringArray stringValues = records.Column(stringFieldIndex) as StringArray;

            int characterCount = 0;

            for (int i = 0; i < stringValues.Length; ++i)
            {
                string current = stringValues.GetString(i);
                characterCount += current.Length;
            }

            var groupFieldIndex = records.Schema.GetFieldIndex(groupFieldName);
            var groupField = records.Schema.GetFieldByIndex(groupFieldIndex);

            return new RecordBatch(
                new Schema.Builder()
                    .Field(f => f.Name(groupField.Name).DataType(groupField.DataType))
                    .Field(f => f.Name(stringFieldName + "_CharCount").DataType(Int32Type.Default))
                    .Build(),
                new IArrowArray[]
                {
                    records.Column(groupFieldIndex),
                    CreateArrowArray(characterCount)
                },
                length: 1);
            ;
        }

        private static IArrowArray CreateArrowArray(int value)
        {
            return new DoubleArray(
                new ArrowBuffer.Builder<int>().Append(value).Build(),
                ArrowBuffer.Empty,
                length: 1,
                nullCount: 0,
                offset: 0);
        }
    }
}
