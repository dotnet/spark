// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.Examples.Sql

open System
open Microsoft.Spark.Examples
open Microsoft.Spark.Sql

type Basic() =
    member this.Run(args : string[]) =
        match args with
        | [| filePath |] ->
            let spark = SparkSession.Builder().AppName("Hello F#").GetOrCreate()

            let df = spark.Read().Json(filePath)

            let schema = df.Schema()
            printfn "%s" (schema.SimpleString)

            for row in df.Collect() do
                printfn "%s" (row.ToString())
        
            df.Show()

            df.PrintSchema()

            df.Select("name", "age", "age", "name").Show()

            df.Select(df.["name"], df.["age"] + 1).Show()

            df.Filter(df.["age"].Gt(21)).Show()
        
            df.GroupBy("age")
                .Agg(Functions.Avg(df.["age"]),
                        Functions.Avg(df.["age"]),
                        Functions.CountDistinct(df.["age"], df.["age"]))
                        .Show()

            // SQL example.
            df.CreateOrReplaceTempView("people")

            // Registering UDF for SQL expression.
            let sqlDf = spark.Sql("SELECT * FROM people")
            sqlDf.Show()

            spark.Udf().Register<Nullable<int>, string, string>(
                "my_udf",
                fun age name ->
                    name + " with " + (if age.HasValue then (string)(age.Value) else "null"))

            let sqlDf = spark.Sql("SELECT my_udf(*) FROM people")
            sqlDf.Show()

            // Using UDF via data frames.
            let addition = Functions.Udf<Nullable<int>, string, string>(
                            fun age name ->
                                name + " is " +
                                    (if age.HasValue then (string)(age.Value + 10) else "0"))

            df.Select(addition.Invoke(df.["age"], df.["name"])).Show()

            // Chaining example:
            let addition2 = Functions.Udf<string, string>(fun str -> "hello " + str + "!")
            df.Select(addition2.Invoke(addition.Invoke(df.["age"], df.["name"]))).Show()

            // Multiple UDF example:
            df.Select(addition.Invoke(df.["age"], df.["name"]), addition2.Invoke(df.["name"]))
                .Show()

            // Joins.
            let joinedDf = df.Join(df, "name")
            joinedDf.Show()

            let joinedDf2 = df.Join(df, ["name"; "age"] |> List.toSeq)
            joinedDf2.Show()

            let joinedDf3 = df.Join(df, df.["name"].EqualTo(df.["name"]), "outer")
            joinedDf3.Show()
            
            // Union of two data frames
            let unionDf = df.Union(df)
            unionDf.Show()

            // Add new column to data frame
            df.WithColumn("location", Functions.Lit("Seattle")).Show()

            // Rename existing column
            df.WithColumnRenamed("name", "fullname").Show()

            // Filter rows with null age
            df.Filter(df.["age"].IsNull()).Show()

            // Fill null values in age column with -1
            df.Na().Fill(-1L, ["age"]).Show()

            // Drop age column
            df.Drop(df.["age"]).Show()

            spark.Stop()
            0
        | _ ->
            printfn "Usage: Basic <path to SPARK_HOME/examples/src/main/resources/people.json>"
            1

    interface IExample with
        member this.Run (args) = this.Run (args)
