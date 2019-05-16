// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using System.Text.RegularExpressions;
using System.IO;
using System.Reflection;

namespace Microsoft.Spark.Scenarios
{
    public class Program
    {
        #region Cloud Run
        public const string StorageConfigKey
            = "fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net";
        public const string SecureStorageKey = "<your-storage-account-key>";
        public const string CloudStoragePath
            = "abfss://<filesystem>@<storage-account>.dfs.core.windows.net/<path>/";
        #endregion

        #region Local Run
        public static string LocalDataStoragePath =
            Path.Combine(
                Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
                "Resources" + Path.DirectorySeparatorChar); 
        #endregion

        static void Main(string[] args)
        {
            if ((args.Length == 0))
            {
                PrintUsage();
                return;
            }

            string StoragePath = args[0] == "local" ? LocalDataStoragePath : CloudStoragePath;

            Builder sparkBuilder = SparkSession.Builder();

            if (args[0] == "cloud")
                sparkBuilder = sparkBuilder.Config(StorageConfigKey, SecureStorageKey);

            SparkSession spark = sparkBuilder
                    .AppName(@"Github 💖 .NET for Apache Spark")
                    .GetOrCreate();

            // Initialize all dataframes (which point to CSV files on the storage system)
            DataFrame projects = spark
                    .Read()
                    .Option("header", "true")
                    .Schema("id INT, url STRING, owner_id INT, name STRING, " +
                            "descriptor STRING, language STRING, created_at STRING, " +
                            "forked_from INT, deleted STRING, updated_at STRING")
                    .Csv(StoragePath + "projects.csv")
                    .Filter(Col("language") == "C#");

            DataFrame watchers = spark
                    .Read()
                    .Option("header", "true")
                    .Schema("repo_id INT, user_id INT, created_at TIMESTAMP")
                    .Csv(StoragePath + "watchers.csv");

            DataFrame commits = spark
                    .Read()
                    .Option("header", "true")
                    .Schema("id INT, sha STRING, author_id INT, committer_id INT, " +
                            "project_id INT, created_at TIMESTAMP")
                    .Csv(StoragePath + "commits.csv");

            // Use functional programming to find top C# projects by stars
            DataFrame stars = projects
              .Join(watchers, Col("id") == watchers["repo_id"])
              .GroupBy("name")
              .Agg(Count("*").Alias("stars"))
              .OrderBy(Desc("stars"));

            stars.Show();

            // Or... Spark SQL!!!
            projects.CreateOrReplaceTempView("projects");
            watchers.CreateOrReplaceTempView("watchers");
            spark.Sql(@"
                    SELECT name, COUNT(*) AS stars 
                    FROM projects 
                    INNER JOIN watchers w 
                    ON id = w.repo_id 
                    GROUP BY name 
                    ORDER BY stars DESC")
                 .Show();

            // Let's figure out the top projects (w.r.t., stars)
            // and how developer commit pattern looks like over a week - 
            // do people work more over weekdays or weekends? :)
            DataFrame projects_aliased = projects
                .As("projects_aliased")
                .Select(Col("id").As("p_id"),
                        Col("name").As("p_name"),
                        Col("language"),
                        Col("created_at").As("p_created_at"));

            DataFrame patterns = commits
                .Join(projects_aliased, commits["project_id"] == projects_aliased["p_id"])
                .Join(stars.Limit(10), Col("name") == projects_aliased["p_name"])
                .Select(DayOfWeek(Col("created_at")).Alias("commit_day"),
                        Col("id").As("commit_id"),
                        Col("p_name").Alias("project_name"),
                        Col("stars"))
                .GroupBy(Col("project_name"), Col("commit_day"), Col("stars"))
                .Agg(Count(Col("commit_id")).Alias("commits"))
                .OrderBy(Asc("project_name"), Asc("commit_day"))
                .Select(Col("project_name"),
                        Col("commit_day"),
                        Col("commits"),
                        Col("stars"));

            patterns.Show();
        }

        private static void PrintUsage()
        {
            string assemblyName = Assembly.GetExecutingAssembly().GetName().Name;
            Console.WriteLine($"Usage: {assemblyName} <local|cloud>");
        }
    }
}
