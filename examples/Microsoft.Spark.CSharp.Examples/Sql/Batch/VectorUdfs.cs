// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using DataFrame = Microsoft.Spark.Sql.DataFrame;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;
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

        private static FxDataFrame CountCharacters(
            FxDataFrame dataFrame,
            string groupFieldName,
            string stringFieldName)
        {
            int characterCount = 0;

            PrimitiveDataFrameColumn<int> characterCountColumn = new PrimitiveDataFrameColumn<int>(stringFieldName + "CharCount");
            PrimitiveDataFrameColumn<int> ageColumn = new PrimitiveDataFrameColumn<int>(groupFieldName);
            ArrowStringDataFrameColumn fieldColumn = dataFrame[stringFieldName] as ArrowStringDataFrameColumn;
            for (long i = 0; i < dataFrame.Rows.Count; i++)
            {
                characterCount += fieldColumn[i].Length;
            }

            if (dataFrame.Rows.Count > 0)
            {
                characterCountColumn.Append(characterCount);
                ageColumn.Append((int?)dataFrame[groupFieldName][0]);
            }

            FxDataFrame ret = new FxDataFrame(new List<DataFrameColumn> { ageColumn, characterCountColumn });
            return ret;
        }
    }
}
