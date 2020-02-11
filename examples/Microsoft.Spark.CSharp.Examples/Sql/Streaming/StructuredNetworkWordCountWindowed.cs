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
    /// spark/examples/src/main/python/sql/streaming/structured_network_wordcount_windowed.py
    ///
    /// You can set up the data source as follow in a separated terminal:
    /// `$ nc -lk 9999`
    /// to start writing standard input to port 9999.
    /// </summary>
    internal sealed class StructuredNetworkWordCountWindowed : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 3 && args.Length != 4)
            {
                Console.Error.WriteLine(
                    "Usage: StructuredNetworkWordCountWindowed " +
                    "<hostname> <port> <window duration in seconds> " +
                    "[<slide duration in seconds>]");
                Environment.Exit(1);
            }

            string hostname = args[0];
            int port = int.Parse(args[1]);
            int windowSize = int.Parse(args[2]);
            int slideSize = (args.Length == 3) ? windowSize : int.Parse(args[3]);
            if (slideSize > windowSize)
            {
                Console.Error.WriteLine(
                    "<slide duration> must be less than or equal " +
                    "to <window duration>");
            }
            string windowDuration = $"{windowSize} seconds";
            string slideDuration = $"{slideSize} seconds";

            SparkSession spark = SparkSession
                .Builder()
                .AppName("StructuredNetworkWordCountWindowed")
                .GetOrCreate();

            DataFrame lines = spark
                .ReadStream()
                .Format("socket")
                .Option("host", hostname)
                .Option("port", port)
                .Option("includeTimestamp", true)
                .Load();

            DataFrame words = lines
                .Select(Explode(Split(lines["value"], " "))
                    .Alias("word"), lines["timestamp"]);
            DataFrame windowedCounts = words
                .GroupBy(Window(words["timestamp"], windowDuration, slideDuration),
                    words["word"])
                .Count()
                .OrderBy("window");

            Spark.Sql.Streaming.StreamingQuery query = windowedCounts
                .WriteStream()
                .OutputMode("complete")
                .Format("console")
                .Option("truncate", false)
                .Start();

            query.AwaitTermination();
        }
    }
}
