// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.Examples.Sql.Streaming

open Microsoft.Spark.Examples
open Microsoft.Spark.Sql

/// <summary>
/// The example is taken/modified from
/// spark/examples/src/main/python/sql/streaming/structured_network_wordcount_windowed.py
///
/// You can set up the data source as follow in a separated terminal:
/// `$ nc -lk 9999`
/// to start writing standard input to port 9999.
/// </summary>
type StructuredNetworkWordCountWindowed() =
    member this.Run(args : string[]) =
        match args with
        | ([| hostname; portStr; windowSizeStr |] | [| hostname; portStr; windowSizeStr; _ |]) ->
            let port = portStr |> int64
            let windowSize = windowSizeStr |> int64
            let slideSize = if (args.Length = 3) then windowSize else (args.[3] |> int64)
            if (slideSize > windowSize) then
                printfn "<slide duration> must be less than or equal to <window duration>"
            let windowDuration = sprintf "%d seconds" windowSize
            let slideDuration = sprintf "%d seconds" slideSize

            let spark =
                SparkSession.Builder().AppName("StructuredNetworkWordCountWindowed").GetOrCreate()

            let lines =
                spark.ReadStream()
                    .Format("socket")
                    .Option("host", hostname)
                    .Option("port", port)
                    .Option("includeTimestamp", true)
                    .Load()

            let words =
                lines.Select(Functions.Explode(Functions.Split(lines.["value"], " "))
                    .Alias("word"), lines.["timestamp"])
            let windowedCounts =
                words.GroupBy(Functions.Window(words.["timestamp"], windowDuration, slideDuration),
                        words.["word"])
                    .Count()
                    .OrderBy("window")

            let query =
                windowedCounts.WriteStream()
                    .OutputMode("complete")
                    .Format("console")
                    .Option("truncate", false)
                    .Start()

            query.AwaitTermination()
            0
        | _ ->
            printfn "Usage: StructuredNetworkWordCountWindowed \
                     <hostname> <port> <window duration in seconds> \
                     [<slide duration in seconds>]"
            1

    interface IExample with
        member this.Run (args) = this.Run (args)
