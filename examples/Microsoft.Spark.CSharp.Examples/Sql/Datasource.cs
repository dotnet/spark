// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Policy;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Examples.Sql
{
    /// <summary>
    ///     A simple example demonstrating Spark SQL data sources
    ///     A copy of spark/examples/src/main/python/sql/datasource.py
    ///
    ///     To run the jdbc example you need a valid jdb reference and driver, as an example to connect to postgres:
    ///         - download the progress jdbc driver (https://jdbc.postgresql.org/download.html) 
    ///         - add the jar to the jars you pass to spark-submit
    ///         - as well as the path to the SPARK_HOME resources, also include the database:
    ///             - jdbc url
    ///             - username
    ///             - password
    ///             - the name of a table to read from
    ///             - the name of a table to write data back to
    ///
    ///     spark-submit --class org.apache.spark.deploy.DotnetRunner `
    ///                  --jars postgresql-42.2.5.jar `
    ///                  --driver-class-path postgresql-42.2.5.jar `
    ///                  --master local c:\git\spark\src\scala\microsoft-spark-2.4.x\target\microsoft-spark-2.4.x-0.1.0.jar `
    ///                  Microsoft.Spark.CSharp.Examples.exe Sql.Datasource `
    ///                  %SPARK_HOME%\examples\src\main\resources\ `
    ///                  jdbc:postgresql:postgres `
    ///                  postgres `
    ///                  fred `
    ///                  spark.table `
    ///                  spark.write_table
    /// 
    /// </summary>
    internal sealed class Datasource : IExample
    {

        public void Run(string[] args)
        {
            if (args.Length != 1 && args.Length != 6)
            {
                Console.Error.WriteLine(
                    "Usage: Datasource <path to SPARK_HOME/examples/src/main/resources/> [jdbc url] [username] [password] [read table] [write table]");
                Console.Error.WriteLine(
                    "To execute the jdbc example, include a jdbc url and a table to read data from and a table to append the data to, if the write table doesn't exist then spark will create the table");
                Console.Error.WriteLine("Example: `spark-submit --class org.apache.spark.deploy.DotnetRunner --jars postgresql-42.2.5.jar --driver-class-path postgresql-42.2.5.jar --master local c:\\github\\dornet-spark\\src\\scala\\microsoft-spark-2.4.x\\target\\microsoft-spark-2.4.x-0.1.0.jar Microsoft.Spark.CSharp.Examples.exe Sql.Datasource  %SPARK_HOME%\\examples\\src\\main\\resources\\ jdbc:postgresql:postgres username password table write_table`");
                Console.Error.WriteLine("The example uses a table definition that looks like: `create table test(id int, name varchar(20));`");

                Environment.Exit(1);
            }

            string parquet = Path.Combine(args[0], "users.parquet");
            string json = Path.Combine(args[0], "people.json");
            string csv = Path.Combine(args[0], "people.csv");
            string orc = Path.Combine(args[0], "users.orc");

            SparkSession spark = SparkSession
                .Builder()
                .AppName(".NET Spark SQL Datasource example")
                .Config("spark.some.config.option", "some-value")
                .GetOrCreate();

            RunBasicDatasourceExample(spark, parquet, json, csv, orc);

            RunParquetExample(spark, json);

            if (args.Length == 6)
            {
                RunDatasourceExample(spark, args[1], args[2], args[3], args[4], args[5]);
            }           

            spark.Stop();
        }

        private void RunDatasourceExample(SparkSession spark, string url, string user, string password, string readTable, string writeTable)
        {
            DataFrame jdbcDf = spark.Read()
                                    .Format("jdbc")
                                    .Options(
                                        new Dictionary<string, string>
                                        {
                                            {"url", url},
                                            {"dbtable", readTable},
                                            {"user", user},
                                            {"password", password}
                                        })
                                    .Load();

            jdbcDf.Show();

            DataFrame jdbcDf2 = spark.Read()
                                    .Format("jdbc")
                                    .Options(
                                        new Dictionary<string, string>
                                        {
                                            {"url", url},
                                            {"dbtable", readTable},
                                            {"user", user},
                                            {"password", password},
                                            {"customSchema", "another_id int, another_name STRING" }
                                        })
                                    .Load();

            jdbcDf2.Show();

            jdbcDf.Write()
                .Format("jdbc")
                .Options(
                    new Dictionary<string, string>
                    {
                        {"url", url},
                        {"dbtable", writeTable},
                        {"user", user},
                        {"password", password}
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
