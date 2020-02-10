// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.Examples.Sql.Streaming
{
    /// <summary>
    /// The example is taken/modified from
    /// spark/examples/src/main/python/sql/streaming/structured_network_wordcount.py
    ///
    /// You can set up the data source as follow in a separated terminal:
    /// `$ nc -lk 9999`
    /// to start writing standard input to port 9999.
    /// </summary>
    internal sealed class StructuredNetworkWordCount : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 2)
            {
                Console.Error.WriteLine(
                    "Usage: StructuredNetworkWordCount <hostname> <port>");
                Environment.Exit(1);
            }

            string hostname = args[0];
            int port = int.Parse(args[1]);

            SparkSession spark = SparkSession
                .Builder()
                .AppName("StructuredNetworkWordCount")
                .GetOrCreate();

            DataFrame lines = spark
                .ReadStream()
                .Format("socket")
                .Option("host", hostname)
                .Option("port", port)
                .Load();

            DataFrame words = lines
                .Select(Explode(Split(lines["value"], " "))
                    .Alias("word"));
            DataFrame wordCounts = words.GroupBy("word").Count();

            Spark.Sql.Streaming.StreamingQuery query = wordCounts
                .WriteStream()
                .OutputMode("complete")
                .Format("console")
                .Start();

            query.AwaitTermination();
        }
    }
}
