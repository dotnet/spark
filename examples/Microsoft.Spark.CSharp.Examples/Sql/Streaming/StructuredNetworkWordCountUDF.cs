// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.Examples.Sql.Streaming
{
    /// <summary>
    /// The example is modified from
    /// spark/examples/src/main/python/sql/streaming/structured_network_wordcount.py
    ///
    /// You can set up the data source as follow in a separated terminal:
    /// `$ nc -lk 9999`
    /// to start writing standard input to port 9999.
    /// </summary>
    internal sealed class StructuredNetworkWordCountUDF : IExample
    {
        public void Run(string[] args)
        {
            // Default to running on localhost:9999
            string hostname = "localhost";
            var port = 9999;

            // User designated their own host and port
            if (args.Length == 2)
            {
                hostname = args[0];
                port = int.Parse(args[1]);
            }

            SparkSession spark = SparkSession
                .Builder()
                .AppName("Streaming example with a UDF")
                .GetOrCreate();

            DataFrame lines = spark
                .ReadStream()
                .Format("socket")
                .Option("host", hostname)
                .Option("port", port)
                .Load();

            // UDF to produce an array
            // Array includes: original string, length of original string
            Func<Column, Column> udfArray =
                Udf<string, string[]>((str) => new string[] { str, str + " " + (str.Length).ToString() });
            DataFrame arrayDF = lines.Select(Explode(udfArray(lines["value"])));

            // Process and display each incoming line
            Microsoft.Spark.Sql.Streaming.StreamingQuery query = arrayDF
                .WriteStream()
                .Format("console")
                .Start();
                
            query.AwaitTermination();
        }
    }
}
