// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using Microsoft.Spark.Sql;

namespace Tpch
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            if (args.Length != 4)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("\t<spark-submit> --master local");
                Console.WriteLine("\t\t--class org.apache.spark.deploy.DotnetRunner <path-to-microsoft-spark-jar>");
                Console.WriteLine("\t\tTpch.exe <tpch_data_root_path> <query_number> <num_iterations> <true for SQL | false for functional>");
            }

            var tpchRoot = args[0];
            var queryNumber = int.Parse(args[1]);
            var numIteration = int.Parse(args[2]);
            var isSQL = bool.Parse(args[3]);

            SparkSession spark = SparkSession
                .Builder()
                .AppName("TPC-H Benchmark for DotNet")
                .GetOrCreate();

            for (var i = 0; i < numIteration; ++i)
            {
                Stopwatch sw = Stopwatch.StartNew();
                Stopwatch swFunc = new Stopwatch();
                if (!isSQL)
                {
                    var tpchFunctional = new TpchFunctionalQueries(tpchRoot, spark);
                    swFunc.Start();
                    tpchFunctional.Run(queryNumber.ToString());
                    swFunc.Stop();
                }
                else
                {
                    var tpchSql = new TpchSqlQueries(tpchRoot, spark);
                    tpchSql.Run(queryNumber.ToString());
                }
                sw.Stop();

                var typeStr = isSQL ? "SQL" : "Functional";
                Console.WriteLine($"TPCH_Result,DotNet,{typeStr},{queryNumber},{i},{sw.ElapsedMilliseconds},{swFunc.ElapsedMilliseconds}");
            }

            spark.Stop();
        }
    }
}
