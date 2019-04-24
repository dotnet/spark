/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package com.microsoft.tpch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
 * Note that the queries are taken from: https://github.com/ssavvides/tpch-spark and updated.
 */
class TpchFunctionalQueries(spark: SparkSession, tpchRoot: String)
  extends TpchBase(spark, tpchRoot) {

  import spark.implicits._

  def run(queryNumber: String): Unit = {
    val method = this.getClass.getMethod("q" + queryNumber)
    method.invoke(this)
  }

  def q1(): Unit = {
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    lineitem.filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
        sum(decrease($"l_extendedprice", $"l_discount")),
        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus")
      .show()
  }

  def q2(): Unit = {
    val europe = region.filter($"r_name" === "EUROPE")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))

    val brass = part.filter(part("p_size") === 15 && part("p_type").endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === $"p_partkey")

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .show()
  }

  def q3(): Unit = {
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val fcust = customer.filter($"c_mktsegment" === "BUILDING")
    val forders = order.filter($"o_orderdate" < "1995-03-15")
    val flineitems = lineitem.filter($"l_shipdate" > "1995-03-15")

    fcust.join(forders, $"c_custkey" === forders("o_custkey"))
      .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
      .join(flineitems, $"o_orderkey" === flineitems("l_orderkey"))
      .select($"l_orderkey",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"o_orderdate", $"o_shippriority")
      .groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc, $"o_orderdate")
      .show()
  }

  def q4(): Unit = {
    val forders = order.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")
    val flineitems = lineitem.filter($"l_commitdate" < $"l_receiptdate")
      .select($"l_orderkey")
      .distinct

    flineitems.join(forders, $"l_orderkey" === forders("o_orderkey"))
      .groupBy($"o_orderpriority")
      .agg(count($"o_orderpriority"))
      .sort($"o_orderpriority")
      .show()
  }

  def q5(): Unit = {
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val forders = order.filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")

    region.filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders, $"l_orderkey" === forders("o_orderkey"))
      .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)
      .show()
  }

  def q6(): Unit = {
    lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05 && $"l_discount" <= 0.07 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))
      .show()
  }

  def q7(): Unit = {
    val getYear = udf { x: String => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val fnation = nation.filter($"n_name" === "FRANCE" || $"n_name" === "GERMANY")
    val fline = lineitem.filter($"l_shipdate" >= "1995-01-01" && $"l_shipdate" <= "1996-12-31")

    val supNation = fnation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(fline, $"s_suppkey" === fline("l_suppkey"))
      .select($"n_name".as("supp_nation"), $"l_orderkey", $"l_extendedprice", $"l_discount", $"l_shipdate")

    fnation.join(customer, $"n_nationkey" === customer("c_nationkey"))
      .join(order, $"c_custkey" === order("o_custkey"))
      .select($"n_name".as("cust_nation"), $"o_orderkey")
      .join(supNation, $"o_orderkey" === supNation("l_orderkey"))
      .filter($"supp_nation" === "FRANCE" && $"cust_nation" === "GERMANY"
        || $"supp_nation" === "GERMANY" && $"cust_nation" === "FRANCE")
      .select($"supp_nation", $"cust_nation",
        getYear($"l_shipdate").as("l_year"),
        decrease($"l_extendedprice", $"l_discount").as("volume"))
      .groupBy($"supp_nation", $"cust_nation", $"l_year")
      .agg(sum($"volume").as("revenue"))
      .sort($"supp_nation", $"cust_nation", $"l_year")
      .show()
  }

  def q8(): Unit = {
    val getYear = udf { x: String => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val isBrazil = udf { (x: String, y: Double) => if (x == "BRAZIL") y else 0 }

    val fregion = region.filter($"r_name" === "AMERICA")
    val forder = order.filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
    val fpart = part.filter($"p_type" === "ECONOMY ANODIZED STEEL")

    val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val line = lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
      decrease($"l_extendedprice", $"l_discount").as("volume")).
      join(fpart, $"l_partkey" === fpart("p_partkey"))
      .join(nat, $"l_suppkey" === nat("s_suppkey"))

    nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
      .select($"n_nationkey")
      .join(customer, $"n_nationkey" === customer("c_nationkey"))
      .select($"c_custkey")
      .join(forder, $"c_custkey" === forder("o_custkey"))
      .select($"o_orderkey", $"o_orderdate")
      .join(line, $"o_orderkey" === line("l_orderkey"))
      .select(getYear($"o_orderdate").as("o_year"), $"volume",
        isBrazil($"n_name", $"volume").as("case_volume"))
      .groupBy($"o_year")
      .agg(sum($"case_volume") / sum("volume"))
      .sort($"o_year")
      .show()
  }

  def q9(): Unit = {
    val getYear = udf { x: String => x.substring(0, 4) }
    val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }

    val linePart = part.filter($"p_name".contains("green"))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    linePart.join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"n_name", getYear($"o_orderdate").as("o_year"),
        expr($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"))
      .groupBy($"n_name", $"o_year")
      .agg(sum($"amount"))
      .sort($"n_name", $"o_year".desc)
      .show()
  }

  def q10(): Unit = {
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val flineitem = lineitem.filter($"l_returnflag" === "R")

    order.filter($"o_orderdate" < "1994-01-01" && $"o_orderdate" >= "1993-10-01")
      .join(customer, $"o_custkey" === customer("c_custkey"))
      .join(nation, $"c_nationkey" === nation("n_nationkey"))
      .join(flineitem, $"o_orderkey" === flineitem("l_orderkey"))
      .select($"c_custkey", $"c_name",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment")
      .groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc)
      .show()
  }

  def q11(): Unit = {
    val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { x: Double => x * 0.0001 }

    val tmp = nation.filter($"n_name" === "GERMANY")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"))

    tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc)
      .show()
  }

  def q12(): Unit = {
    val highPriority = udf { x: String => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
    val lowPriority = udf { x: String => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

    lineitem.filter((
      $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(sum(highPriority($"o_orderpriority")).as("sum_highorderpriority"),
        sum(lowPriority($"o_orderpriority")).as("sum_loworderpriority"))
      .sort($"l_shipmode")
      .show()
  }

  def q13(): Unit = {
    val special = udf { x: String => x.matches(".*special.*requests.*") }

    customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"c_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"*").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)
      .show()
  }

  def q14(): Unit = {
    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))
      .show()
  }

  def q15(): Unit = {
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val revenue = lineitem.filter($"l_shipdate" >= "1996-01-01" &&
      $"l_shipdate" < "1996-04-01")
      .select($"l_suppkey", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"l_suppkey")
      .agg(sum($"value").as("total"))

    revenue.agg(max($"total").as("max_total"))
      .join(revenue, $"max_total" === revenue("total"))
      .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total")
      .sort($"s_suppkey")
      .show()
  }

  def q16(): Unit = {
    val complains = udf { x: String => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { x: String => x.startsWith("MEDIUM POLISHED") }
    val numbers = udf { x: Int => x.toString.matches("49|14|23|45|19|3|36|9") }

    val fparts = part.filter(($"p_brand" =!= "Brand#45") && !polished($"p_type") &&
      numbers($"p_size"))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    supplier.filter(!complains($"s_comment"))
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")
      .show()
  }

  def q17(): Unit = {
    val mul02 = udf { x: Double => x * 0.2 }

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")

    fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)
      .show()
  }

  def q18(): Unit = {
    lineitem.groupBy($"l_orderkey")
      .agg(sum($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("key"), $"sum_quantity")
      .join(order, order("o_orderkey") === $"key")
      .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
      .join(customer, customer("c_custkey") === $"o_custkey")
      .select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .agg(sum("l_quantity"))
      .sort($"o_totalprice".desc, $"o_orderdate")
      .show()
  }

  def q19(): Unit = {
    val sm = udf { x: String => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
    val md = udf { x: String => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
    val lg = udf { x: String => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#12") &&
          sm($"p_container") &&
          $"l_quantity" >= 1 && $"l_quantity" <= 11 &&
          $"p_size" >= 1 && $"p_size" <= 5) ||
          (($"p_brand" === "Brand#23") &&
            md($"p_container") &&
            $"l_quantity" >= 10 && $"l_quantity" <= 20 &&
            $"p_size" >= 1 && $"p_size" <= 10) ||
          (($"p_brand" === "Brand#34") &&
            lg($"p_container") &&
            $"l_quantity" >= 20 && $"l_quantity" <= 30 &&
            $"p_size" >= 1 && $"p_size" <= 15))
      .select(decrease($"l_extendedprice", $"l_discount").as("volume"))
      .agg(sum("volume"))
      .show()
  }

  def q20(): Unit = {
    val forest = udf { x: String => x.startsWith("forest") }

    val flineitem = lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01")
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((sum($"l_quantity") * 0.5).as("sum_quantity"))

    val fnation = nation.filter($"n_name" === "CANADA")
    val nat_supp = supplier.select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address")
      .join(fnation, $"s_nationkey" === fnation("n_nationkey"))

    part.filter(forest($"p_name"))
      .select($"p_partkey").distinct
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(flineitem, $"ps_suppkey" === flineitem("l_suppkey") && $"ps_partkey" === flineitem("l_partkey"))
      .filter($"ps_availqty" > $"sum_quantity")
      .select($"ps_suppkey").distinct
      .join(nat_supp, $"ps_suppkey" === nat_supp("s_suppkey"))
      .select($"s_name", $"s_address")
      .sort($"s_name")
      .show()
  }

  def q21(): Unit = {
    val fsupplier = supplier.select($"s_suppkey", $"s_nationkey", $"s_name")

    val plineitem = lineitem.select($"l_suppkey", $"l_orderkey", $"l_receiptdate", $"l_commitdate")

    val flineitem = plineitem.filter($"l_receiptdate" > $"l_commitdate")

    val line1 = plineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val line2 = flineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val forder = order.select($"o_orderkey", $"o_orderstatus")
      .filter($"o_orderstatus" === "F")

    nation.filter($"n_name" === "SAUDI ARABIA")
      .join(fsupplier, $"n_nationkey" === fsupplier("s_nationkey"))
      .join(flineitem, $"s_suppkey" === flineitem("l_suppkey"))
      .join(forder, $"l_orderkey" === forder("o_orderkey"))
      .join(line1, $"l_orderkey" === line1("key"))
      .filter($"suppkey_count" > 1 || ($"suppkey_count" == 1 && $"l_suppkey" == $"max_suppkey"))
      .select($"s_name", $"l_orderkey", $"l_suppkey")
      .join(line2, $"l_orderkey" === line2("key"), "left_outer")
      .select($"s_name", $"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
      .filter($"suppkey_count" === 1 && $"l_suppkey" === $"suppkey_max")
      .groupBy($"s_name")
      .agg(count($"l_suppkey").as("numwait"))
      .sort($"numwait".desc, $"s_name")
      .show()
  }

  def q22(): Unit = {
    val sub2 = udf { x: String => x.substring(0, 2) }
    val phone = udf { x: String => x.matches("13|31|23|29|30|18|17") }

    val fcustomer = customer.select($"c_acctbal", $"c_custkey", sub2($"c_phone").as("cntrycode"))
      .filter(phone($"cntrycode"))

    val avg_customer = fcustomer.filter($"c_acctbal" > 0.0)
      .agg(avg($"c_acctbal").as("avg_acctbal"))

    order.groupBy($"o_custkey")
      .agg($"o_custkey").select($"o_custkey")
      .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
      .filter($"o_custkey".isNull)
      .join(avg_customer)
      .filter($"c_acctbal" > $"avg_acctbal")
      .groupBy($"cntrycode")
      .agg(count($"c_acctbal"), sum($"c_acctbal"))
      .sort($"cntrycode")
      .show()
  }
}