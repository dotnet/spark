// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.Examples.Sql.Streaming

open Microsoft.Spark.Examples
open Microsoft.Spark.Sql

/// <summary>
/// The example is taken/modified from
/// spark/examples/src/main/python/sql/streaming/structured_network_wordcount.py
///
/// You can set up the data source as follow in a separated terminal:
/// `$ nc -lk 9999`
/// to start writing standard input to port 9999.
/// </summary>
type StructuredNetworkWordCount() =
    member this.Run(args : string[]) =
        match args with
        | [| hostname; portStr |] ->
            let port = portStr |> int64

            let spark = SparkSession.Builder().AppName("StructuredNetworkWordCount").GetOrCreate()

            let lines =
                spark.ReadStream()
                    .Format("socket")
                    .Option("host", hostname)
                    .Option("port", port)
                    .Load()

            let words =
                lines.Select(Functions.Explode(Functions.Split(lines.["value"], " "))
                    .Alias("word"))
            let wordCounts = words.GroupBy("word").Count()

            let query = wordCounts.WriteStream().OutputMode("complete").Format("console").Start()

            query.AwaitTermination()

            0
        | _ ->
            printfn "Usage: StructuredNetworkWordCount <hostname> <port>"
            1

    interface IExample with
        member this.Run (args) = this.Run (args)
