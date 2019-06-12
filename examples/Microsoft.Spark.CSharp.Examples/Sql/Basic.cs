// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
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
            DataFrame df = spark.Read().Schema("age INT, name DOUBLE").Json(args[0]);

            Spark.Sql.Types.StructType schema = df.Schema();
            Console.WriteLine(schema.SimpleString);

            IEnumerable<Row> rows = df.Collect();
            foreach (Row row in rows)
            {
                Console.WriteLine(row);
            }

            df.Show();

            df.PrintSchema();

            //System.Diagnostics.Debugger.Launch();

            // Using UDF via data frames.
            Func<RecordBatch, RecordBatch> sumChars = batch => SubtractMean(batch, "name");

            df.GroupBy("age")
                .Apply(df.Schema(), sumChars)
                .Show();

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
            //    Udf<string, string[]>((str) => new string[] { str, str + str });
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

            spark.Stop();
        }

        private static RecordBatch SubtractMean(RecordBatch pdf, string fieldName)
        {
            var fieldIndex = pdf.Schema.GetFieldIndex(fieldName);
            DoubleArray values = pdf.Column(fieldIndex) as DoubleArray;

            ArrowBuffer.Builder<double> builder = new ArrowBuffer.Builder<double>(values.Length);
            double mean = Mean(values.Values);
            foreach (double value in values.Values)
            {
                builder.Append(value - mean);
            }

            var newArrays = pdf.Arrays
                .Select((a, index) => index != fieldIndex ? a : CreateArray(builder));
            return new RecordBatch(pdf.Schema, newArrays, pdf.Length);
        }

        private static IArrowArray CreateArray(ArrowBuffer.Builder<double> builder)
        {
            return new DoubleArray(builder.Build(), ArrowBuffer.Empty, builder.Length, 0, 0);
        }

        private static double Mean(ReadOnlySpan<double> values)
        {
            return Sum(values) / values.Length;
        }

        private static double Sum(ReadOnlySpan<double> values)
        {
            double result = 0;
            foreach (double value in values)
            {
                result += value;
            }
            return result;
        }
    }
}
