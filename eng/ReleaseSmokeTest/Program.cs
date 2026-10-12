// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace SparkReleaseSmokeTest
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                throw new ArgumentException("Specify the expected Spark version.");
            }

            SparkSession spark = SparkSession.Builder()
                .AppName("Spark release package smoke test")
                .GetOrCreate();
            try
            {
                string version = spark.Version();
                if (version != args[0])
                {
                    throw new InvalidOperationException(
                        $"Expected Spark {args[0]}, but connected to {version}.");
                }

                DataFrame input = spark.Range(0, 8, 1, 2);
                if (input.Count() != 8)
                {
                    throw new InvalidOperationException("Driver Count returned an unexpected result.");
                }

                long[] expected = Enumerable.Range(0, 8).Select(value => (long)value).ToArray();
                CheckRows(input, expected, "Collect");

                // The UDF lives in this published application, not the repository's test assemblies.
                Func<Column, Column> increment = Udf<long, long>(value => value + 1);
                CheckRows(input.Select(increment(input["id"])),
                    expected.Select(value => value + 1).ToArray(), "Scalar .NET UDF");
            }
            finally
            {
                spark.Stop();
            }

            Console.WriteLine("SPARK_RELEASE_SMOKE_TEST_PASSED");
        }

        private static void CheckRows(DataFrame frame, long[] expected, string operation)
        {
            long[] actual = frame.Collect().Select(row => row.GetAs<long>(0)).OrderBy(value => value).ToArray();
            if (!actual.SequenceEqual(expected))
            {
                throw new InvalidOperationException($"{operation} returned unexpected rows.");
            }

            Console.WriteLine($"{operation}: {actual.Length} rows verified.");
        }
    }
}
