// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Examples.Sql.Batch
{
    /// <summary>
    /// The example is taken/modified from spark/examples/src/main/python/sql/datasource.py
    /// </summary>
    internal sealed class Datasource : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 1)
            {
                Console.Error.WriteLine(
                    "Usage: Datasource <path to SPARK_HOME/examples/src/main/resources/>");
                
                Environment.Exit(1);
            }

            string parquet = Path.Combine(args[0], "users.parquet");
            string json = Path.Combine(args[0], "people.json");
            string csv = Path.Combine(args[0], "people.csv");
            string orc = Path.Combine(args[0], "users.orc");

            SparkSession spark = SparkSession
                .Builder()
                .AppName("SQL Datasource example using .NET for Apache Spark")
                .Config("spark.some.config.option", "some-value")
                .GetOrCreate();

            RunBasicDatasourceExample(spark, parquet, json, csv, orc);

            RunParquetExample(spark, json);

            RunDatasourceExample(spark);

            spark.Stop();
        }

        private void RunDatasourceExample(SparkSession spark)
        {
            DataFrame jdbcDf = spark.Read()
                .Format("jdbc")
                .Options(
                    new Dictionary<string, string>
                    {
                        {"url", "jdbc:postgresql:postgres"},
                        {"dbtable", "table_name"},
                        {"user", "user_name"},
                        {"password", "password"}
                    })
                .Load();

            jdbcDf.Show();

            DataFrame jdbcDf2 = spark.Read()
                .Format("jdbc")
                .Options(
                    new Dictionary<string, string>
                    {
                        {"url", "jdbc:postgresql:postgres"},
                        {"dbtable", "table_name"},
                        {"user", "user_name"},
                        {"password", "password"},
                        {"customSchema", "another_id int, another_name STRING" }
                    })
                .Load();

            jdbcDf2.Show();

            jdbcDf.Write()
                .Format("jdbc")
                .Options(
                    new Dictionary<string, string>
                    {
                        {"url", "jdbc:postgresql:postgres"},
                        {"dbtable", "table_name"},
                        {"user", "user_name"},
                        {"password", "password"}
                    })
                .Mode(SaveMode.Append)
                .Save();
        }

        private void RunParquetExample(SparkSession spark, string json)
        {
            DataFrame peopleDf = spark.Read().Json(json);
        
            peopleDf.Write().Mode(SaveMode.Overwrite).Parquet("people.parquet");

            DataFrame parquetFile = spark.Read().Parquet("people.parquet");
            
            parquetFile.CreateTempView("parquet");

            DataFrame teenagers = spark.Sql(
                "SELECT name FROM parquet WHERE age >= 13 and age <= 19");

            teenagers.Show();
        }

        private void RunBasicDatasourceExample(SparkSession spark, string parquet, string json, string csv, string orc)
        {
            DataFrame df = spark.Read().Load(parquet);

            df.PrintSchema();

            df.Select("name", "favorite_color")
                .Write()
                .Mode(SaveMode.Overwrite)
                .Save("namesPartByColor.parquet");

            df.Write()
                .Mode(SaveMode.Overwrite)
                .PartitionBy("favorite_color")
                .BucketBy(42, "name")
                .SaveAsTable("people_partitioned_bucketed");

            df = spark.Read().Format("json").Load(json);

            df.PrintSchema();

            df.Select("name", "age")
                .Write()
                .Mode(SaveMode.Overwrite)
                .Format("parquet")
                .Save("namesAndAges.parquet");

            df = spark.Read()
                .Format("csv")
                .Option("sep", ";")
                .Option("inferSchema", true)
                .Option("header", true)
                .Load(csv);

            df.PrintSchema();

            df = spark.Read().Orc(orc);

            df.Write()
                .Format("orc")
                .Options(new Dictionary<string, string>
                {
                    {"orc.bloom.filter.columns", "favorite_color"},
                    {"orc.dictionary.key.threshold", "1.0"},
                    {"orc.column.encoding.direct", "name"}
                })
                .Mode(SaveMode.Overwrite)
                .Save("users_with_options.orc");

            df.Write()
                .BucketBy(42, "name")
                .SortBy("favorite_color")
                .SaveAsTable("people_bucketed");

            spark.Sql($"SELECT * FROM parquet.`{parquet}`").Show();

            spark.Sql("SELECT * FROM people_bucketed").Show();
            spark.Sql("SELECT * FROM people_partitioned_bucketed").Show();

            spark.Sql("DROP TABLE IF EXISTS people_bucketed");
            spark.Sql("DROP TABLE IF EXISTS people_partitioned_bucketed");
        }
    }
}
