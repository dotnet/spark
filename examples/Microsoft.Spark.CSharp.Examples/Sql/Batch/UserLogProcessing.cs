// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Microsoft.Spark.Examples.Sql.Batch
{
    /// <summary>
    /// An example demonstrating log processing using user defined functions, regular expressions, and Spark SQL.
    /// </summary>
    internal sealed class ApacheUserLogProcessing : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 1)
            {
                Console.Error.WriteLine(
                    "Usage: ApacheUserLogProcessing <path to Apache User Logs>");
                Environment.Exit(1);
            }

            SparkSession spark = SparkSession
                .Builder()
                .AppName("Apache User Log Processing")
                .GetOrCreate();

            // Read input log file and display it
            var df = spark.Read().Text(args[0]);
            df.Show();

            // Register UDF to perform regex on each row of input file
            spark.Udf().Register<string, string, bool>(
                "GeneralReg", 
                (log, type) => RegTest(log, type));
            df.CreateOrReplaceTempView("Logs");
            DataFrame generalDf = spark.Sql(
                "SELECT logs.value, GeneralReg(logs.value, 'genfilter') FROM Logs");
            generalDf.Show();

            // Only store log entries that matched the reg ex
            generalDf = generalDf.Filter(generalDf["GeneralReg(value, genfilter)"] == true);
            generalDf.Show();
            generalDf.PrintSchema();

            // Only choose log entries that start with 10
            spark.Udf().Register<string, string, bool>(
                "IPReg", 
                (log, type) => RegTest(log, type));
            generalDf.CreateOrReplaceTempView("IPLogs");
            DataFrame ipDf = spark.Sql(
                "SELECT iplogs.value, IPReg(iplogs.value, 'ipfilter') FROM IPLogs");
            ipDf.Show();

            ipDf = ipDf.Filter(ipDf["IPReg(value, ipfilter)"] == true); 
            ipDf.Show();

            // After choosing entries starting with 10, find entries that deal with spam
            spark.Udf().Register<string, string, bool>(
                "SpamRegEx", 
                (log, type) => RegTest(log, type));
            ipDf.CreateOrReplaceTempView("SpamLogs");
            DataFrame spamDF = spark.Sql(
                "SELECT spamlogs.value, SpamRegEx(spamlogs.value, 'spamfilter') FROM SpamLogs");
            spamDF.Show();

            // Only keep entries that related to spam
            DataFrame trueSpam = spamDF.Filter(spamDF["SpamRegEx(value, spamfilter)"] == true);
            trueSpam.CreateOrReplaceTempView("TrueSpamLogs");
            DataFrame trueSpamSql = spark.Sql(
                "SELECT truespamlogs.value FROM truespamlogs");

            // Explore the columns in the data we have been filtering
            IEnumerable<Row> rows = trueSpamSql.Collect();
            foreach(Row row in rows)
            {
                string rowstring = row.ToString();
                ParseLog(rowstring);
            }

            spark.Stop();
        }

        public static bool RegTest(string logLine, string regexType)
        {            
            // Example Apache log line:   
            // 64.242.88.10 - - [07/Mar/2004:16:47:12 -0800] "GET /robots.txt HTTP/1.1" 200 68
            // 1:IP   2:client   3:user   4:date time   5:method   
            // 6:req   7:proto   8:respcode   9:size
            Regex useRx = new Regex(
                    "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)");

            // Which regex to use based on what we're filtering for
            if(regexType == "ipfilter")
            {
                useRx = new Regex("^(?=10)");
            }
            else if(regexType == "spamfilter")
            {
                useRx = new Regex("\\b(?=spam)\\b");
            }

            if(useRx.IsMatch(logLine))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static void ParseLog(string logLine)
        {
            Match match = Regex.Match(
                logLine, 
                "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)");

            int groupCtr = 0;
            int entryCtr = 0;

            string[] colNames = 
                {"ip", 
                "client", 
                "user", 
                "date", 
                "method", 
                "endpt", 
                "protocol", 
                "response", 
                "size"};

            // Print out the full log line and then divide based on column
            foreach(Group group in match.Groups)
            {
                if(groupCtr == 0)
                {
                    Console.WriteLine("Full log entry: '{0}'", group.Value);
                    ++groupCtr;
                    continue;
                }
                Console.WriteLine("    {0}: '{1}'", colNames[entryCtr], group.Value);
                ++groupCtr;
                ++entryCtr;
            }
        }
    }    
}
                