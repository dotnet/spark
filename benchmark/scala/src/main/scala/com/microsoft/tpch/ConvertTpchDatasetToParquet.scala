/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

// TODO(laserljy): fix the comment below. 
// The code is modified based on Wentao's code. Wentao's homepage: https://www.microsoft.com/en-us/research/people/wentwu/

package mypackage

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ConvertTpchCsvToParquetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      // .master(s"local[*]") // UnComment this line to run locally.
      .appName(this.getClass.getName)
      .getOrCreate()

    val tpchCsvRootFolderPath = args(1)
    val tpchParquetRootFolderPath = args(2)

    // Convert CSV files to Parquet files.
    for (i <- TpchSchema.tpchTables.indices) {
      val df = spark
        .read
        .format("csv")
        .option("delimiter", "|")
        .schema(TpchSchema.tpchTables(i)._2)
        .load(tpchCsvRootFolderPath + "/" + TpchSchema.tpchTables(i)._1 + ".tbl")

      df.write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .save(tpchParquetRootFolderPath + "/" + TpchSchema.tpchTables(i)._1)
    }
  }
}

object TpchSchema {
  val customer = StructType(Array(
    StructField("c_custkey", IntegerType),
    StructField("c_name", StringType),
    StructField("c_address", StringType),
    StructField("c_nationkey", IntegerType),
    StructField("c_phone", StringType),
    StructField("c_acctbal", DoubleType),
    StructField("c_mktsegment", StringType),
    StructField("c_comment", StringType)
  ))

  val lineitem = StructType(Array(
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
    StructField("l_comment", StringType)
  ))

  val nation = StructType(Array(
    StructField("n_nationkey", IntegerType),
    StructField("n_name", StringType),
    StructField("n_regionkey", IntegerType),
    StructField("n_comment", StringType)
  ))

  val orders = StructType(Array(
    StructField("o_orderkey", IntegerType),
    StructField("o_custkey", IntegerType),
    StructField("o_orderstatus", StringType),
    StructField("o_totalprice", DoubleType),
    StructField("o_orderdate", StringType),
    StructField("o_orderpriority", StringType),
    StructField("o_clerk", StringType),
    StructField("o_shippriority", IntegerType),
    StructField("o_comment", StringType)
  ))

  val part = StructType(Array(
    StructField("p_partkey", IntegerType),
    StructField("p_name", StringType),
    StructField("p_mfgr", StringType),
    StructField("p_brand", StringType),
    StructField("p_type", StringType),
    StructField("p_size", IntegerType),
    StructField("p_container", StringType),
    StructField("p_retailprice", DoubleType),
    StructField("p_comment", StringType)
  ))

  val partsupp = StructType(Array(
    StructField("ps_partkey", IntegerType),
    StructField("ps_suppkey", IntegerType),
    StructField("ps_availqty", IntegerType),
    StructField("ps_supplycost", DoubleType),
    StructField("ps_comment", StringType)
  ))

  val region = StructType(Array(
    StructField("r_regionkey", IntegerType),
    StructField("r_name", StringType),
    StructField("r_comment", StringType)
  ))

  val supplier = StructType(Array(
    StructField("s_suppkey", IntegerType),
    StructField("s_name", StringType),
    StructField("s_address", StringType),
    StructField("s_nationkey", IntegerType),
    StructField("s_phone", StringType),
    StructField("s_acctbal", DoubleType),
    StructField("s_comment", StringType)
  ))

  // (name, table, bucketing column)
  val tpchTables = Array(
    ("customer", customer, customer.head.name),
    ("lineitem", lineitem, lineitem.head.name),
    ("nation", nation, nation.head.name),
    ("orders", orders, orders.head.name),
    ("part", part, part.head.name),
    ("partsupp", partsupp, partsupp.head.name),
    ("region", region, region.head.name),
    ("supplier", supplier, supplier.head.name)
  )
}
