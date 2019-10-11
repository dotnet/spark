// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using StructType = Microsoft.Spark.Sql.Types.StructType;

namespace Microsoft.Spark.Examples.Batch
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
                .AppName(".NET Spark SQL VectorUdfs example")
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
                    r => CountCharacters(r, "age", "name"))
                .Show();

            spark.Stop();
        }

        private static RecordBatch CountCharacters(
            RecordBatch records,
            string groupFieldName,
            string stringFieldName)
        {
            int stringFieldIndex = records.Schema.GetFieldIndex(stringFieldName);
            StringArray stringValues = records.Column(stringFieldIndex) as StringArray;

            int characterCount = 0;

            for (int i = 0; i < stringValues.Length; ++i)
            {
                string current = stringValues.GetString(i);
                characterCount += current.Length;
            }

            int groupFieldIndex = records.Schema.GetFieldIndex(groupFieldName);
            Field groupField = records.Schema.GetFieldByIndex(groupFieldIndex);

            // Return 1 record, if we were given any. 0, otherwise.
            int returnLength = records.Length > 0 ? 1 : 0;

            return new RecordBatch(
                new Schema.Builder()
                    .Field(groupField)
                    .Field(f => f.Name(stringFieldName + "_CharCount").DataType(Int32Type.Default))
                    .Build(),
                new IArrowArray[]
                {
                    records.Column(groupFieldIndex),
                    new Int32Array.Builder().Append(characterCount).Build()
                },
                returnLength);
        }
    }
}
