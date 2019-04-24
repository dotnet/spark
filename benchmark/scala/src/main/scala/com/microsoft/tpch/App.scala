/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package com.microsoft.tpch

import scala.util.Try

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage:")
      println("\t<spark-submit> --master local --class com.microsoft.tpch.App microsoft-spark-examples-<version>.jar")
      println("\t\t<tpch_data_root_path> <query_number> <num_iterations> <true for SQL | false for functional>")
    }

    val tpchRoot = args(0)
    val queryNumber = args(1).toInt
    val numIteration = args(2).toInt
    val isSql = Try(args(3).toBoolean).getOrElse(false)

    for (i <- 0 until numIteration) {
      val spark = SparkSession
        .builder()
        .appName("TPC-H Benchmark for Scala")
        .getOrCreate()

      val startTs = System.currentTimeMillis

      if (!isSql) {
        val tpchFunctional = new TpchFunctionalQueries(spark, tpchRoot)
        tpchFunctional.run(queryNumber.toString)
      }
      else {
      }

      val endTs = System.currentTimeMillis
      val totalTime = endTs - startTs

      val typeStr = if (isSql) "SQL"
      else "Functional"

      println(s"TPCH_Result,Scala,$typeStr,$queryNumber,$i,$totalTime")

      spark.stop()
    }
  }
}
