// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Examples.Sql.Batch
{
    /// <summary>
    /// An example demonstrating log processing.
    /// Includes UDFs, regular expressions, and Spark SQL.
    /// </summary>
    internal sealed class Logging : IExample
    {
        /* Regular expression for log pattern matching later
           Example Apache log line:   
           64.242.88.10 - - [07/Mar/2004:16:47:12 -0800] "GET /robots.txt HTTP/1.1" 200 68
           1:IP   2:client   3:user   4:date time   5:method   
           6:req   7:proto   8:respcode   9:size */
        static readonly string s_apacheRx =
                "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";

        public void Run(string[] args)
        {
            if (args.Length != 1)
            {
                Console.Error.WriteLine(
                    "Usage: Logging <path to Apache User Logs>");
                Environment.Exit(1);
            }

            SparkSession spark = SparkSession
                .Builder()
                .AppName("Apache User Log Processing")
                .GetOrCreate();

            // Read input log file and display it
            DataFrame df = spark.Read().Text(args[0]);
            df.Show();

            // Step 1: UDF to determine if each line is a valid log entry
            // Remove any invalid entries before further filtering
            spark.Udf().Register<string, bool>(
                "GeneralReg",
                log => Regex.IsMatch(log, s_apacheRx));

            df.CreateOrReplaceTempView("Logs");

            // Apply the UDF to get valid log entries
            DataFrame generalDf = spark.Sql(
                "SELECT logs.value, GeneralReg(logs.value) FROM Logs");

            // Only keep log entries that matched the reg ex
            generalDf = generalDf.Filter(generalDf["GeneralReg(value)"]);
            generalDf.Show();

            // View the resulting schema
            // Notice we created a new column "GeneralReg(value)"
            generalDf.PrintSchema();

            // Step 2: Choose valid log entries that start with 10
            spark.Udf().Register<string, bool>(
                "IPReg",
                log => Regex.IsMatch(log, "^(?=10)"));

            generalDf.CreateOrReplaceTempView("IPLogs");

            // Apply UDF to get valid log entries starting with 10
            // Use SQL "WHERE" rather than doing ipDf.Filter(),
            // which avoids creating an extra column "IPReg(value)"
            DataFrame ipDf = spark.Sql(
                "SELECT iplogs.value FROM IPLogs WHERE IPReg(iplogs.value)");
            ipDf.Show();

            // Step 3: Choose valid log entries that start 
            // with 10 and deal with spam
            spark.Udf().Register<string, bool>(
                "SpamRegEx",
                log => Regex.IsMatch(log, "\\b(?=spam)\\b"));

            ipDf.CreateOrReplaceTempView("SpamLogs");

            // Apply UDF to get valid, start with 10, spam entries
            DataFrame spamDF = spark.Sql(
                "SELECT spamlogs.value FROM SpamLogs WHERE SpamRegEx(spamlogs.value)");

            // Let's explore the columns in the data we have filtered
            // Use LINQ to count the number of GET requests
            int numGetRequests = spamDF
                .Collect()
                .Where(r => ContainsGet(r.GetAs<string>("value")))
                .Count();

            Console.WriteLine("Number of GET requests: " + numGetRequests);

            spark.Stop();
        }

        public static bool ContainsGet(string logLine)
        {
            // Use regex matching to group data
            // Each group matches a column in our log schema
            // i.e. first group = first column =  IP
            Match match = Regex.Match(logLine, s_apacheRx);

            // Determine if valid log entry is a GET request
            if (match.Success)
            {
                Console.WriteLine("Full log entry: '{0}'", match.Groups[0].Value);

                // 5th column/group in schema is "method"
                if (match.Groups[5].Value == "GET")
                {
                    return true;
                }
            }

            return false;
        }
    }
}
