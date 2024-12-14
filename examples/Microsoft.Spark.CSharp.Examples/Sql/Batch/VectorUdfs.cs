// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using IntegerType = Microsoft.Spark.Sql.Types.IntegerType;
using StructType = Microsoft.Spark.Sql.Types.StructType;

namespace Microsoft.Spark.Examples.Sql.Batch
{
    /// <summary>
    /// An example demonstrating basic Spark SQL features.
    /// </summary>
    internal sealed class VectorUdfs : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 1)
            {
                Console.Error.WriteLine(
                    "Usage: Sql.VectorUdfs <path to SPARK_HOME/examples/src/main/resources/people.json>");
                Environment.Exit(1);
            }

            SparkSession spark = SparkSession
                .Builder()
                // Lower the shuffle partitions to speed up groupBy() operations.
                .Config("spark.sql.shuffle.partitions", "3")
                .AppName("SQL VectorUdfs example using .NET for Apache Spark")
                .GetOrCreate();

            DataFrame df = spark.Read().Schema("age INT, name STRING").Json(args[0]);

            StructType schema = df.Schema();
            Console.WriteLine(schema.SimpleString);

            df.Show();

            df.PrintSchema();

            // Grouped Map Vector UDF
            // able to return different shapes and record lengths
            df.GroupBy("age")
                .Apply(
                    new StructType(new[]
                    {
                        new StructField("age", new IntegerType()),
                        new StructField("nameCharCount", new IntegerType())
                    }),
                    r => CountCharacters(r))
                .Show();

            spark.Stop();
        }

        private static RecordBatch CountCharacters(RecordBatch records)
        {
            StringArray nameColumn = records.Column("name") as StringArray;

            int characterCount = 0;

            for (int i = 0; i < nameColumn.Length; ++i)
            {
                string current = nameColumn.GetString(i);
                characterCount += current.Length;
            }

            int ageFieldIndex = records.Schema.GetFieldIndex("age");
            Field ageField = records.Schema.GetFieldByIndex(ageFieldIndex);

            // Return 1 record, if we were given any. 0, otherwise.
            int returnLength = records.Length > 0 ? 1 : 0;

            return new RecordBatch(
                new Schema.Builder()
                    .Field(ageField)
                    .Field(f => f.Name("name_CharCount").DataType(Int32Type.Default))
                    .Build(),
                new IArrowArray[]
                {
                    records.Column(ageFieldIndex),
                    new Int32Array.Builder().Append(characterCount).Build()
                },
                returnLength);
        }
    }
}
