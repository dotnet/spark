// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.Sql;


namespace Microsoft.Spark.Examples.Sql
{
    /// <summary>
    /// A simple example demonstrating Spark SQL data sources
    ///
    ///  A copy of spark/examples/src/main/python/sql/datasource.py
    /// 
    /// /// </summary>
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

            var parquet = Path.Combine(args[0], "users.parquet");
            var json = Path.Combine(args[0], "people.json");
            var csv = Path.Combine(args[0], "people.csv");
            var orc = Path.Combine(args[0], "users.orc");

            SparkSession spark = SparkSession
                .Builder()
                .AppName(".NET Spark SQL basic example")
                .Config("spark.some.config.option", "some-value")
                .GetOrCreate();


            var df = spark.Read().Load(parquet);

            df.PrintSchema();

            df.Select("name", "favorite_color").Write().Mode(SaveMode.Overwrite).Save("namesPartByColor.parquet");

            df.Write()
                .Mode(SaveMode.Overwrite)
            //    .PartitionBy("favorite_color")    //BUG: Issue 55 - can't find partitionBy -  a fix is in a PR, will update this when PR56 is merged 
                .BucketBy(42, "name")
                .SaveAsTable("people_partitioned_bucketed");

            df = spark.Read().Format("json").Load(json);
            df.PrintSchema();
            df.Select("name", "age").Write().Mode(SaveMode.Overwrite).Format("parquet").Save("namesAndAges.parquet");

            df = spark.Read().Format("csv").Option("sep", ";").Option("inferSchema", true).Option("header", true).Load(csv);

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


            df.Write().BucketBy(42, "name").SortBy("favorite_color").SaveAsTable("people_bucketed");


            spark.Sql($"SELECT * FROM parquet.`{parquet}`").Show();

            spark.Sql("SELECT * FROM people_bucketed").Show();
            spark.Sql("SELECT * FROM people_partitioned_bucketed").Show();

            spark.Sql("DROP TABLE IF EXISTS people_bucketed");
            spark.Sql("DROP TABLE IF EXISTS people_partitioned_bucketed");

            spark.Stop();
        }
    }
}
