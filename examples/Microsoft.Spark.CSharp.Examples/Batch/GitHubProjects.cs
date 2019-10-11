// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.Examples.Batch
{
    /// <summary>
    /// An example analyzing GitHub projects
    /// data, demonstrating Spark SQL features.
    /// </summary>
    internal sealed class GitHubProjects : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 1)
            {
                Console.Error.WriteLine( 
                    "Usage: GitHubProjects <path to projects.csv>");
                Environment.Exit(1);
            }

            SparkSession spark = SparkSession
                .Builder()
                .AppName("GitHub and Spark Batch")
                .GetOrCreate();

            DataFrame projectsDf = spark
              .Read()
              .Schema("id INT, url STRING, owner_id INT, name STRING, descriptor STRING, language STRING, created_at STRING, forked_from INT, deleted STRING, updated_at STRING")
              .Csv(args[0]);

            projectsDf.Show();

            // Drop any rows with NA values
            DataFrameNaFunctions dropEmptyProjects = projectsDf.Na();
            DataFrame cleanedProjects = dropEmptyProjects.Drop("any");

            // Remove unnecessary columns
            cleanedProjects = cleanedProjects.Drop("id", "url", "owner_id");
            cleanedProjects.Show();

            // Average number of times each language has been forked
            DataFrame groupedDF = cleanedProjects
                .GroupBy("language")
                .Agg(Avg(cleanedProjects["forked_from"]));

            // Sort by most forked languages first
            groupedDF.OrderBy(Desc("avg(forked_from)")).Show();

            // Find projects updated since 10/20/15
            spark.Udf().Register<string, bool>(
                "MyUDF", 
                (date) => DateTest(date));

            cleanedProjects.CreateOrReplaceTempView("dateView");

            DataFrame dateDf = spark.Sql(
                "SELECT *, MyUDF(dateView.updated_at) AS datebefore FROM dateView");
            dateDf.Show();

            spark.Stop();
        }

        public static bool DateTest(string date)
        {
            // Remove invalid dates to avoid: 
            // System.FormatException: String '0000-00-00 00:00:00' 
            // was not recognized as a valid DateTime
            if (date.Equals("0000-00-00 00:00:00"))
            {
                return false;
            }

            DateTime convertedDate = Convert.ToDateTime(date);

            // 10/20/2015 
            DateTime referenceDate = new DateTime(2015, 10, 20);

            // > 0 means convertedDate (input from file) is later than 10/20/15
            if (DateTime.Compare(convertedDate, referenceDate) > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
