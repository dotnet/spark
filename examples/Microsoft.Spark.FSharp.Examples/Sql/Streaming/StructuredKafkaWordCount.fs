// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.Examples.Sql.Streaming

open Microsoft.Spark.Examples
open Microsoft.Spark.Sql

/// <summary>
/// The example is taken/modified from
/// spark/examples/src/main/python/sql/streaming/structured_kafka_wordcount.py
/// </summary>
type StructuredKafkaWordCount() =
    member this.Run(args : string[]) =
        match args with
        | [| bootstrapServers; subscribeType; topics |] ->
            let spark = SparkSession.Builder().AppName("StructuredKafkaWordCount").GetOrCreate()

            let lines =
                spark.ReadStream()
                    .Format("kafka")
                    .Option("kafka.bootstrap.servers", bootstrapServers)
                    .Option(subscribeType, topics)
                    .Load()
                    .SelectExpr("CAST(value AS STRING)")

            let words =
                lines.Select(Functions.Explode(Functions.Split(lines.["value"], " "))
                    .Alias("word"))
            let wordCounts = words.GroupBy("word").Count()

            let query = wordCounts.WriteStream().OutputMode("complete").Format("console").Start()

            query.AwaitTermination()

            0
        | _ ->
            printfn "Usage: StructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>"
            1

    interface IExample with
        member this.Run (args) = this.Run (args)
