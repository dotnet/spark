/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package com.microsoft.tpch

import org.apache.spark.sql.{DataFrame, SparkSession}

class TpchBase(spark: SparkSession, tpchRoot: String) {
  val customer: DataFrame = spark.read.parquet(s"${tpchRoot}customer")
  val lineitem: DataFrame = spark.read.parquet(s"${tpchRoot}lineitem")
  val nation: DataFrame = spark.read.parquet(s"${tpchRoot}nation")
  val order: DataFrame = spark.read.parquet(s"${tpchRoot}orders")
  val part: DataFrame = spark.read.parquet(s"${tpchRoot}part")
  val partsupp: DataFrame = spark.read.parquet(s"${tpchRoot}partsupp")
  val region: DataFrame = spark.read.parquet(s"${tpchRoot}region")
  val supplier: DataFrame = spark.read.parquet(s"${tpchRoot}supplier")
}
