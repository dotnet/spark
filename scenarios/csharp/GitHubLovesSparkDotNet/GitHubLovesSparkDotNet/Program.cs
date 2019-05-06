// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using System.Text.RegularExpressions;

namespace Microsoft.Spark.Scenarios
{
    // Application presented at Microsoft //Build 2019:
    // 1. By Scott Hanselman & Scott Hunter in the following session:
    //    BRK3015 - .NET Platform Overview and Roadmap
    //    https://mybuild.techcommunity.microsoft.com/sessions/77031?source=sessions
    // 2. By Rahul Potharaju in the following session:
    //    BRK3055 - Building data pipelines for Modern Data Warehouse with Spark and .NET in Azure
    //    https://mybuild.techcommunity.microsoft.com/sessions/76996?source=sessions
    //
    // This app is purely for demonstration purposes. And more 
    // importantly, this was not written by a seasoned Data Scientist 
    // so please avoid deriving *any* conclusions from the results :) 
    //
    // Of course, please feel free to open PRs to improve and make 
    // this app more interesting.
    //
    // Acknowledgements:
    // - The GHTorrent project
    //   http://ghtorrent.org
    // - Azure Databricks 
    //   https://azure.microsoft.com/en-us/services/databricks/
    // - Azure Data Lake Storage Gen2 (ABFS) 
    //   https://azure.microsoft.com/en-us/services/storage/data-lake-storage/

    public class Program
    {
        public const string StorageConfigKey = "fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net";
        public const string SecureStorageKey = "<your-storage-account-key>";
        public const string DataStoragePath = "abfss://<filesystem>@<storage-account>.dfs.core.windows.net/<path>/";

        static void Main(string[] args)
        {
            SparkSession spark = SparkSession
                    .Builder()
                    .Config(StorageConfigKey, SecureStorageKey)
                    .AppName(@"Github 💖 .NET for Apache Spark")
                    .GetOrCreate();

            // Initialize all dataframes (which point to CSV files on the storage system)
            DataFrame commits = spark
                    .Read()
                    .Schema("id INT, sha STRING, author_id INT, committer_id INT, " +
                            "project_id INT, created_at TIMESTAMP")
                    .Csv(DataStoragePath + "commits.csv");

            DataFrame watchers = spark
                    .Read()
                    .Schema("repo_id INT, user_id INT, created_at TIMESTAMP")
                    .Csv(DataStoragePath + "watchers.csv");

            DataFrame projects = spark
                    .Read()
                    .Schema("id INT, url STRING, owner_id INT, name STRING, " + 
                            "descriptor STRING, language STRING, created_at STRING, " + 
                            "forked_from INT, deleted STRING, updated_at STRING")
                    .Csv(DataStoragePath + "projects.csv")
                    .Filter(Col("language") == "C#");

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
                    | FROM projects 
                    | INNER JOIN watchers w 
                    | ON id = w.repo_id 
                    | GROUP BY name 
                    | ORDER BY stars DESC".StripMargin())
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
    }

    internal static class StringExtensions
    {
        internal static string StripMargin(this string s)
        {
            return Regex.Replace(s, @"[ \t]+\|", string.Empty);
        }
    }
}
