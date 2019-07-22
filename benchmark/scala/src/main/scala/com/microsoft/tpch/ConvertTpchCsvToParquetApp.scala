/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package com.microsoft.tpch

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ConvertTpchCsvToParquetApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage:")
      println(
        "\t<spark-submit> --master local[*] --class com.microsoft.tpch.ConvertTpchCsvToParquetApp microsoft-" +
          "spark-benchmark-<version>.jar")
      println(
        "\t\t<path-to-source-directory-with-TPCH-tables> <path-to-destination-directory-to-save-parquet-file>")
      return
    }

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    // The 1st arg should be the folder containing TPCH tables.
    val tpchSourceFolderPath = args(0)
    // The 2nd arg should be the destination folder to save TPCH parquet files.
    val tpchDestinationFolderPath = args(1)

    // Convert CSV files to Parquet files.
    TpchSchema.tpchTables.foreach(table => {
      val df = spark.read
        .format("csv")
        .option("delimiter", "|")
        .schema(table._2)
        .load(tpchSourceFolderPath + "/" + table._1 + ".tbl")

      df.write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .save(tpchDestinationFolderPath + "/" + table._1)
    })
  }
}

object TpchSchema {
  val customer = StructType(
    Array(
      StructField("c_custkey", IntegerType),
      StructField("c_name", StringType),
      StructField("c_address", StringType),
      StructField("c_nationkey", IntegerType),
      StructField("c_phone", StringType),
      StructField("c_acctbal", DoubleType),
      StructField("c_mktsegment", StringType),
      StructField("c_comment", StringType)))

  val lineitem = StructType(
    Array(
      StructField("l_orderkey", IntegerType),
      StructField("l_partkey", IntegerType),
      StructField("l_suppkey", IntegerType),
      StructField("l_linenumber", IntegerType),
      StructField("l_quantity", DoubleType),
      StructField("l_extendedprice", DoubleType),
      StructField("l_discount", DoubleType),
      StructField("l_tax", DoubleType),
      StructField("l_returnflag", StringType),
      StructField("l_linestatus", StringType),
      StructField("l_shipdate", StringType),
      StructField("l_commitdate", StringType),
      StructField("l_receiptdate", StringType),
      StructField("l_shipinstruct", StringType),
      StructField("l_shipmode", StringType),
      StructField("l_comment", StringType)))

  val nation = StructType(
    Array(
      StructField("n_nationkey", IntegerType),
      StructField("n_name", StringType),
      StructField("n_regionkey", IntegerType),
      StructField("n_comment", StringType)))

  val orders = StructType(
    Array(
      StructField("o_orderkey", IntegerType),
      StructField("o_custkey", IntegerType),
      StructField("o_orderstatus", StringType),
      StructField("o_totalprice", DoubleType),
      StructField("o_orderdate", StringType),
      StructField("o_orderpriority", StringType),
      StructField("o_clerk", StringType),
      StructField("o_shippriority", IntegerType),
      StructField("o_comment", StringType)))

  val part = StructType(
    Array(
      StructField("p_partkey", IntegerType),
      StructField("p_name", StringType),
      StructField("p_mfgr", StringType),
      StructField("p_brand", StringType),
      StructField("p_type", StringType),
      StructField("p_size", IntegerType),
      StructField("p_container", StringType),
      StructField("p_retailprice", DoubleType),
      StructField("p_comment", StringType)))

  val partsupp = StructType(
    Array(
      StructField("ps_partkey", IntegerType),
      StructField("ps_suppkey", IntegerType),
      StructField("ps_availqty", IntegerType),
      StructField("ps_supplycost", DoubleType),
      StructField("ps_comment", StringType)))

  val region = StructType(
    Array(
      StructField("r_regionkey", IntegerType),
      StructField("r_name", StringType),
      StructField("r_comment", StringType)))

  val supplier = StructType(
    Array(
      StructField("s_suppkey", IntegerType),
      StructField("s_name", StringType),
      StructField("s_address", StringType),
      StructField("s_nationkey", IntegerType),
      StructField("s_phone", StringType),
      StructField("s_acctbal", DoubleType),
      StructField("s_comment", StringType)))

  // (name, table)
  val tpchTables = Array(
    ("customer", customer),
    ("lineitem", lineitem),
    ("nation", nation),
    ("orders", orders),
    ("part", part),
    ("partsupp", partsupp),
    ("region", region),
    ("supplier", supplier))
}
