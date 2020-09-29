// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.Reflection;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.ArrowFunctions;
using static Microsoft.Spark.Sql.DataFrameFunctions;
using static Microsoft.Spark.Sql.Functions;
using Column = Microsoft.Spark.Sql.Column;
using DataFrame = Microsoft.Spark.Sql.DataFrame;

namespace Tpch
{
    internal class TpchFunctionalQueries : TpchBase
    {
        internal TpchFunctionalQueries(string tpchRoot, SparkSession spark)
            : base(tpchRoot, spark)
        {
        }

        internal void RunAll()
        {
            for (var i = 1; i <= 22; i++)
            {
                Run(i.ToString());
            }
        }

        internal void Run(string queryNumber)
        {
            Console.WriteLine($"Spark.NET TPCH Functional Query: #{queryNumber}");
            Type thisType = GetType();
            MethodInfo queryMethod = thisType.GetMethod(
                $"Q{queryNumber}", BindingFlags.Instance | BindingFlags.NonPublic);

            var sw = Stopwatch.StartNew();
            queryMethod.Invoke(this, null);
            Console.WriteLine($"\tElapsed: {sw.Elapsed}");
        }

        internal void Q1()
        {
            Func<Column, Column, Column> decrease = Udf<double, double, double>((x, y) => x * (1 - y));
            Func<Column, Column, Column> increase = Udf<double, double, double>((x, y) => x * (1 + y));

            _lineitem.Filter(Col("l_shipdate") <= "1998-09-02")
              .GroupBy(Col("l_returnflag"), Col("l_linestatus"))
              .Agg(Sum(Col("l_quantity")).As("sum_qty"), Sum(Col("l_extendedprice")).As("sum_base_price"),
                Sum(decrease(Col("l_extendedprice"), Col("l_discount"))).As("sum_disc_price"),
                Sum(increase(decrease(Col("l_extendedprice"), Col("l_discount")), Col("l_tax"))).As("sum_charge"),
                Avg(Col("l_quantity")).As("avg_qty"),
                Avg(Col("l_extendedprice")).As("avg_price"),
                Avg(Col("l_discount")).As("avg_disc"),
                Count(Col("l_quantity")).As("count_order")
               )
              .Sort(Col("l_returnflag"), Col("l_linestatus"))
              .Show();
        }

        internal void Q1d()
        {
            Func<Column, Column, Column> discPrice = VectorUdf<DoubleDataFrameColumn, DoubleDataFrameColumn, DoubleDataFrameColumn>(
                (price, discount) => VectorDataFrameFunctions.ComputeDiscountPrice(price, discount));

            Func<Column, Column, Column, Column> total = VectorUdf<DoubleDataFrameColumn, DoubleDataFrameColumn, DoubleDataFrameColumn, DoubleDataFrameColumn>(
                (price, discount, tax) => VectorDataFrameFunctions.ComputeTotal(price, discount, tax));

            _lineitem.Filter(Col("l_shipdate") <= "1998-09-02")
              .GroupBy(Col("l_returnflag"), Col("l_linestatus"))
              .Agg(Sum(Col("l_quantity")).As("sum_qty"), Sum(Col("l_extendedprice")).As("sum_base_price"),
                Sum(discPrice(Col("l_extendedprice"), Col("l_discount"))).As("sum_disc_price"),
                Sum(total(Col("l_extendedprice"), Col("l_discount"), Col("l_tax"))).As("sum_charge"),
                Avg(Col("l_quantity")).As("avg_qty"),
                Avg(Col("l_extendedprice")).As("avg_price"),
                Avg(Col("l_discount")).As("avg_disc"),
                Count(Col("l_quantity")).As("count_order")
               )
              .Sort(Col("l_returnflag"), Col("l_linestatus"))
              .Show();
        }

        internal void Q1v()
        {
            Func<Column, Column, Column> discPrice = VectorUdf<DoubleArray, DoubleArray, DoubleArray>(
                (price, discount) => VectorFunctions.ComputeDiscountPrice(price, discount));

            Func<Column, Column, Column, Column> total = VectorUdf<DoubleArray, DoubleArray, DoubleArray, DoubleArray>(
                (price, discount, tax) => VectorFunctions.ComputeTotal(price, discount, tax));

            _lineitem.Filter(Col("l_shipdate") <= "1998-09-02")
              .GroupBy(Col("l_returnflag"), Col("l_linestatus"))
              .Agg(Sum(Col("l_quantity")).As("sum_qty"), Sum(Col("l_extendedprice")).As("sum_base_price"),
                Sum(discPrice(Col("l_extendedprice"), Col("l_discount"))).As("sum_disc_price"),
                Sum(total(Col("l_extendedprice"), Col("l_discount"), Col("l_tax"))).As("sum_charge"),
                Avg(Col("l_quantity")).As("avg_qty"),
                Avg(Col("l_extendedprice")).As("avg_price"),
                Avg(Col("l_discount")).As("avg_disc"),
                Count(Col("l_quantity")).As("count_order")
               )
              .Sort(Col("l_returnflag"), Col("l_linestatus"))
              .Show();
        }

        internal void Q2()
        {
            DataFrame europe = _region.Filter(Col("r_name") == "EUROPE")
                .Join(_nation, Col("r_regionkey") == _nation["n_regionkey"])
                .Join(_supplier, Col("n_nationkey") == _supplier["s_nationkey"])
                .Join(_partsupp, _supplier["s_suppkey"] == _partsupp["ps_suppkey"]);

            DataFrame brass = _part
                .Filter(_part["p_size"] == 15 & _part["p_type"].EndsWith("BRASS"))
                .Join(europe, europe["ps_partkey"] == Col("p_partkey"));

            DataFrame minCost = brass.GroupBy(brass["ps_partkey"])
                .Agg(Min("ps_supplycost").As("min"));

            brass.Join(minCost, brass["ps_partkey"] == minCost["ps_partkey"])
                .Filter(brass["ps_supplycost"] == minCost["min"])
                .Select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
                .Sort(Col("s_acctbal").Desc(), Col("n_name"), Col("s_name"), Col("p_partkey"))
                .Show();
        }

        internal void Q3()
        {
            Func<Column, Column, Column> decrease = Udf<double, double, double>((x, y) => x * (1 - y));

            DataFrame fcust = _customer.Filter(Col("c_mktsegment") == "BUILDING");
            DataFrame forders = _orders.Filter(Col("o_orderdate") < "1995-03-15");
            DataFrame flineitems = _lineitem.Filter(Col("l_shipdate") > "1995-03-15");

            fcust.Join(forders, Col("c_custkey") == forders["o_custkey"])
              .Select(Col("o_orderkey"), Col("o_orderdate"), Col("o_shippriority"))
              .Join(flineitems, Col("o_orderkey") == flineitems["l_orderkey"])
              .Select(Col("l_orderkey"),
                decrease(Col("l_extendedprice"), Col("l_discount")).As("volume"),
                Col("o_orderdate"), Col("o_shippriority"))
              .GroupBy(Col("l_orderkey"), Col("o_orderdate"), Col("o_shippriority"))
              .Agg(Sum(Col("volume")).As("revenue"))
              .Sort(Col("revenue").Desc(), Col("o_orderdate"))
              .Show();
        }

        internal void Q4()
        {
            DataFrame forders = _orders.Filter(Col("o_orderdate") >= "1993-07-01" &
                                        Col("o_orderdate") < "1993-10-01");
            DataFrame flineitems = _lineitem.Filter(Col("l_commitdate") < Col("l_receiptdate"))
                                      .Select($"l_orderkey")
                                      .Distinct();

            flineitems.Join(forders, Col("l_orderkey") == forders["o_orderkey"])
              .GroupBy(Col("o_orderpriority"))
              .Agg(Count(Col("o_orderpriority")).As("order_count"))
              .Sort(Col("o_orderpriority"))
              .Show();
        }

        internal void Q5()
        {
            Func<Column, Column, Column> decrease = Udf<double, double, double>((x, y) => x * (1 - y));

            DataFrame forders = _orders.Filter(Col("o_orderdate") < "1995-01-01" & Col("o_orderdate") >= "1994-01-01");

            _region.Filter(Col("r_name") == "ASIA")
              .Join(_nation, Col("r_regionkey") == _nation["n_regionkey"])
              .Join(_supplier, Col("n_nationkey") == _supplier["s_nationkey"])
              .Join(_lineitem, Col("s_suppkey") == _lineitem["l_suppkey"])
              .Select(Col("n_name"), Col("l_extendedprice"), Col("l_discount"), Col("l_orderkey"), Col("s_nationkey"))
              .Join(forders, Col("l_orderkey") == forders["o_orderkey"])
              .Join(_customer, Col("o_custkey") == _customer["c_custkey"]
                & Col("s_nationkey") == _customer["c_nationkey"])
              .Select(Col("n_name"), decrease(Col("l_extendedprice"), Col("l_discount")).As("value"))
              .GroupBy(Col("n_name"))
              .Agg(Sum(Col("value")).As("revenue"))
              .Sort(Col("revenue").Desc())
              .Show();
        }

        internal void Q6()
        {
            _lineitem.Filter(Col("l_shipdate") >= "1994-01-01" & Col("l_shipdate") < "1995-01-01"
              & Col("l_discount") >= 0.05 & Col("l_discount") <= 0.07 & Col("l_quantity") < 24)
              .Agg(Sum(Col("l_extendedprice") * Col("l_discount")).As("revenue"))
              .Show();
        }

        // C#, Scala and SparkSQL results match but SparkSQL has different precision.
        internal void Q7()
        {
            Func<Column, Column> getYear = Udf<string, string>(x => x.Substring(0, 4));
            Func<Column, Column, Column> decrease = Udf<double, double, double>((x, y) => x * (1 - y));

            DataFrame fnation = _nation.Filter(Col("n_name") == "FRANCE" | Col("n_name") == "GERMANY");
            DataFrame fline = _lineitem.Filter(Col("l_shipdate") >= "1995-01-01" & Col("l_shipdate") <= "1996-12-31");

            DataFrame supNation = fnation.Join(_supplier, Col("n_nationkey") == _supplier["s_nationkey"])
              .Join(fline, Col("s_suppkey") == fline["l_suppkey"])
              .Select(Col("n_name").As("supp_nation"), Col("l_orderkey"), Col("l_extendedprice"), Col("l_discount"), Col("l_shipdate"));

            fnation.Join(_customer, Col("n_nationkey") == _customer["c_nationkey"])
              .Join(_orders, Col("c_custkey") == _orders["o_custkey"])
              .Select(Col("n_name").As("cust_nation"), Col("o_orderkey"))
              .Join(supNation, Col("o_orderkey") == supNation["l_orderkey"])
              .Filter(Col("supp_nation") == "FRANCE" & Col("cust_nation") == "GERMANY"
                | Col("supp_nation") == "GERMANY" & Col("cust_nation") == "FRANCE")
              .Select(Col("supp_nation"), Col("cust_nation"),
                getYear(Col("l_shipdate")).As("l_year"),
                decrease(Col("l_extendedprice"), Col("l_discount")).As("volume"))
              .GroupBy(Col("supp_nation"), Col("cust_nation"), Col("l_year"))
              .Agg(Sum(Col("volume")).As("revenue"))
              .Sort(Col("supp_nation"), Col("cust_nation"), Col("l_year"))
              .Show();
        }

        internal void Q8()
        {
            Func<Column, Column> getYear = Udf<string, string>(x => x.Substring(0, 4));
            Func<Column, Column, Column> decrease = Udf<double, double, double>((x, y) => x * (1 - y));
            Func<Column, Column, Column> isBrazil = Udf<string, double, double>((x, y) => x == "BRAZIL" ? y : 0);

            DataFrame fregion = _region.Filter(Col("r_name") == "AMERICA");
            DataFrame forder = _orders.Filter(Col("o_orderdate") <= "1996-12-31" & Col("o_orderdate") >= "1995-01-01");
            DataFrame fpart = _part.Filter(Col("p_type") == "ECONOMY ANODIZED STEEL");

            DataFrame nat = _nation.Join(_supplier, Col("n_nationkey") == _supplier["s_nationkey"]);

            DataFrame line = _lineitem.Select(Col("l_partkey"), Col("l_suppkey"), Col("l_orderkey"),
              decrease(Col("l_extendedprice"), Col("l_discount")).As("volume"))
              .Join(fpart, Col("l_partkey") == fpart["p_partkey"])
              .Join(nat, Col("l_suppkey") == nat["s_suppkey"]);

            _nation.Join(fregion, Col("n_regionkey") == fregion["r_regionkey"])
              .Select(Col("n_nationkey"))
              .Join(_customer, Col("n_nationkey") == _customer["c_nationkey"])
              .Select(Col("c_custkey"))
              .Join(forder, Col("c_custkey") == forder["o_custkey"])
              .Select(Col("o_orderkey"), Col("o_orderdate"))
              .Join(line, Col("o_orderkey") == line["l_orderkey"])
              .Select(getYear(Col("o_orderdate")).As("o_year"), Col("volume"),
                isBrazil(Col("n_name"), Col("volume")).As("case_volume"))
              .GroupBy(Col("o_year"))
              .Agg((Sum(Col("case_volume")) / Sum("volume")).As("mkt_share"))
              .Sort(Col("o_year"))
              .Show();
        }

        internal void Q8d()
        {
            Func<Column, Column> getYear = Udf<string, string>(x => x.Substring(0, 4));
            Func<Column, Column, Column> discPrice = VectorUdf<DoubleDataFrameColumn, DoubleDataFrameColumn, DoubleDataFrameColumn>(
                (price, discount) => VectorDataFrameFunctions.ComputeDiscountPrice(price, discount));

            Func<Column, Column, Column> isBrazil = Udf<string, double, double>((x, y) => x == "BRAZIL" ? y : 0);

            DataFrame fregion = _region.Filter(Col("r_name") == "AMERICA");
            DataFrame forder = _orders.Filter(Col("o_orderdate") <= "1996-12-31" & Col("o_orderdate") >= "1995-01-01");
            DataFrame fpart = _part.Filter(Col("p_type") == "ECONOMY ANODIZED STEEL");

            DataFrame nat = _nation.Join(_supplier, Col("n_nationkey") == _supplier["s_nationkey"]);

            DataFrame line = _lineitem.Select(Col("l_partkey"), Col("l_suppkey"), Col("l_orderkey"),
              discPrice(Col("l_extendedprice"), Col("l_discount")).As("volume"))
              .Join(fpart, Col("l_partkey") == fpart["p_partkey"])
              .Join(nat, Col("l_suppkey") == nat["s_suppkey"]);

            _nation.Join(fregion, Col("n_regionkey") == fregion["r_regionkey"])
              .Select(Col("n_nationkey"))
              .Join(_customer, Col("n_nationkey") == _customer["c_nationkey"])
              .Select(Col("c_custkey"))
              .Join(forder, Col("c_custkey") == forder["o_custkey"])
              .Select(Col("o_orderkey"), Col("o_orderdate"))
              .Join(line, Col("o_orderkey") == line["l_orderkey"])
              .Select(getYear(Col("o_orderdate")).As("o_year"), Col("volume"),
                isBrazil(Col("n_name"), Col("volume")).As("case_volume"))
              .GroupBy(Col("o_year"))
              .Agg((Sum(Col("case_volume")) / Sum("volume")).As("mkt_share"))
              .Sort(Col("o_year"))
              .Show();
        }

        internal void Q8v()
        {
            Func<Column, Column> getYear = Udf<string, string>(x => x.Substring(0, 4));
            Func<Column, Column, Column> discPrice = VectorUdf<DoubleArray, DoubleArray, DoubleArray>(
                (price, discount) => VectorFunctions.ComputeDiscountPrice(price, discount));

            Func<Column, Column, Column> isBrazil = Udf<string, double, double>((x, y) => x == "BRAZIL" ? y : 0);

            DataFrame fregion = _region.Filter(Col("r_name") == "AMERICA");
            DataFrame forder = _orders.Filter(Col("o_orderdate") <= "1996-12-31" & Col("o_orderdate") >= "1995-01-01");
            DataFrame fpart = _part.Filter(Col("p_type") == "ECONOMY ANODIZED STEEL");

            DataFrame nat = _nation.Join(_supplier, Col("n_nationkey") == _supplier["s_nationkey"]);

            DataFrame line = _lineitem.Select(Col("l_partkey"), Col("l_suppkey"), Col("l_orderkey"),
              discPrice(Col("l_extendedprice"), Col("l_discount")).As("volume"))
              .Join(fpart, Col("l_partkey") == fpart["p_partkey"])
              .Join(nat, Col("l_suppkey") == nat["s_suppkey"]);

            _nation.Join(fregion, Col("n_regionkey") == fregion["r_regionkey"])
              .Select(Col("n_nationkey"))
              .Join(_customer, Col("n_nationkey") == _customer["c_nationkey"])
              .Select(Col("c_custkey"))
              .Join(forder, Col("c_custkey") == forder["o_custkey"])
              .Select(Col("o_orderkey"), Col("o_orderdate"))
              .Join(line, Col("o_orderkey") == line["l_orderkey"])
              .Select(getYear(Col("o_orderdate")).As("o_year"), Col("volume"),
                isBrazil(Col("n_name"), Col("volume")).As("case_volume"))
              .GroupBy(Col("o_year"))
              .Agg((Sum(Col("case_volume")) / Sum("volume")).As("mkt_share"))
              .Sort(Col("o_year"))
              .Show();
        }

        internal void Q9()
        {
            Func<Column, Column> getYear = Udf<string, string>(x => x.Substring(0, 4));
            Func<Column, Column, Column, Column, Column> expr = Udf<double, double, double, double, double>((x, y, v, w) => x * (1 - y) - (v * w));

            DataFrame linePart = _part.Filter(Col("p_name").Contains("green"))
                               .Join(_lineitem, Col("p_partkey") == _lineitem["l_partkey"]);

            DataFrame natSup = _nation.Join(_supplier, Col("n_nationkey") == _supplier["s_nationkey"]);

            linePart.Join(natSup, Col("l_suppkey") == natSup["s_suppkey"])
              .Join(_partsupp, Col("l_suppkey") == _partsupp["ps_suppkey"]
                & Col("l_partkey") == _partsupp["ps_partkey"])
              .Join(_orders, Col("l_orderkey") == _orders["o_orderkey"])
              .Select(Col("n_name"), getYear(Col("o_orderdate")).As("o_year"),
                expr(Col("l_extendedprice"), Col("l_discount"), Col("ps_supplycost"), Col("l_quantity")).As("amount"))
              .GroupBy(Col("n_name"), Col("o_year"))
              .Agg(Sum(Col("amount")))
              .Sort(Col("n_name"), Col("o_year").Desc())
              .Show();
        }

        internal void Q10()
        {
            Func<Column, Column, Column> decrease = Udf<double, double, double>((x, y) => x * (1 - y));

            DataFrame flineitem = _lineitem.Filter(Col("l_returnflag") == "R");

            _orders.Filter(Col("o_orderdate") < "1994-01-01" & Col("o_orderdate") >= "1993-10-01")
              .Join(_customer, Col("o_custkey") == _customer["c_custkey"])
              .Join(_nation, Col("c_nationkey") == _nation["n_nationkey"])
              .Join(flineitem, Col("o_orderkey") == flineitem["l_orderkey"])
              .Select(Col("c_custkey"), Col("c_name"),
                decrease(Col("l_extendedprice"), Col("l_discount")).As("volume"),
                Col("c_acctbal"), Col("n_name"), Col("c_address"), Col("c_phone"), Col("c_comment"))
              .GroupBy(Col("c_custkey"), Col("c_name"), Col("c_acctbal"), Col("c_phone"), Col("n_name"), Col("c_address"), Col("c_comment"))
              .Agg(Sum(Col("volume")).As("revenue"))
              .Sort(Col("revenue").Desc())
              .Show();
        }

        internal void Q11()
        {
            Func<Column, Column, Column> mul = Udf<double, int, double>((x, y) => x * y);
            Func<Column, Column> mul01 = Udf<double, double>(x => x * 0.0001);

            DataFrame tmp = _nation.Filter(Col("n_name") == "GERMANY")
              .Join(_supplier, Col("n_nationkey") == _supplier["s_nationkey"])
              .Select(Col("s_suppkey"))
              .Join(_partsupp, Col("s_suppkey") == _partsupp["ps_suppkey"])
              .Select(Col("ps_partkey"), mul(Col("ps_supplycost"), Col("ps_availqty")).As("value"));

            DataFrame sumRes = tmp.Agg(Sum("value").As("total_value"));

            tmp.GroupBy(Col("ps_partkey")).Agg(Sum("value").As("part_value"))
              .Join(sumRes, Col("part_value") > mul01(Col("total_value")))
              .Sort(Col("part_value").Desc())
              .Show();
        }

        internal void Q12()
        {
            Func<Column, Column, Column> mul = Udf<double, int, double>((x, y) => x * y);
            Func<Column, Column> highPriority = Udf<string, int>(x => (x == "1-URGENT" || x == "2-HIGH") ? 1 : 0);
            Func<Column, Column> lowPriority = Udf<string, int>(x => (x != "1-URGENT" && x != "2-HIGH") ? 1 : 0);

            _lineitem.Filter((
              Col("l_shipmode") == "MAIL" | Col("l_shipmode") == "SHIP") &
              Col("l_commitdate") < Col("l_receiptdate") &
              Col("l_shipdate") < Col("l_commitdate") &
              Col("l_receiptdate") >= "1994-01-01" & Col("l_receiptdate") < "1995-01-01")
              .Join(_orders, Col("l_orderkey") == _orders["o_orderkey"])
              .Select(Col("l_shipmode"), Col("o_orderpriority"))
              .GroupBy(Col("l_shipmode"))
              .Agg(Sum(highPriority(Col("o_orderpriority"))).As("sum_highorderpriority"),
                Sum(lowPriority(Col("o_orderpriority"))).As("sum_loworderpriority"))
              .Sort(Col("l_shipmode"))
              .Show();
        }

        private static readonly Regex s_q13SpecialRegex = new Regex("^.*special.*requests.*", RegexOptions.Compiled);
        internal void Q13()
        {
            Func<Column, Column> special = Udf<string, bool>((x) => s_q13SpecialRegex.IsMatch(x));

            DataFrame c_orders = _customer.Join(_orders, Col("c_custkey") == _orders["o_custkey"]
              & !special(_orders["o_comment"]), "left_outer")
              .GroupBy(Col("c_custkey"))
              .Agg(Count(Col("o_orderkey")).As("c_count"));

            c_orders
            .GroupBy(Col("c_count"))
            .Agg(Count(Col("*")).As("custdist"))
            .Sort(Col("custdist").Desc(), Col("c_count").Desc())
            .Show();
        }

        internal void Q14()
        {
            Func<Column, Column, Column> reduce = Udf<double, double, double>((x, y) => x * (1 - y));
            Func<Column, Column, Column> promo = Udf<string, double, double>((x, y) => x.StartsWith("PROMO") ? y : 0);

            _part.Join(_lineitem, Col("l_partkey") == Col("p_partkey") &
              Col("l_shipdate") >= "1995-09-01" & Col("l_shipdate") < "1995-10-01")
              .Select(Col("p_type"), reduce(Col("l_extendedprice"), Col("l_discount")).As("value"))
              .Agg(Sum(promo(Col("p_type"), Col("value"))) * 100 / Sum(Col("value")))
              .Show();
        }

        internal void Q15()
        {
            Func<Column, Column, Column> decrease = Udf<double, double, double>((x, y) => x * (1 - y));

            DataFrame revenue = _lineitem.Filter(Col("l_shipdate") >= "1996-01-01" &
              Col("l_shipdate") < "1996-04-01")
              .Select(Col("l_suppkey"), decrease(Col("l_extendedprice"), Col("l_discount")).As("value"))
              .GroupBy(Col("l_suppkey"))
              .Agg(Sum(Col("value")).As("total"));

            revenue.Agg(Max(Col("total")).As("max_total"))
              .Join(revenue, Col("max_total") == revenue["total"])
              .Join(_supplier, Col("l_suppkey") == _supplier["s_suppkey"])
              .Select(Col("s_suppkey"), Col("s_name"), Col("s_address"), Col("s_phone"), Col("total"))
              .Sort(Col("s_suppkey"))
              .Show();
        }

        private static readonly Regex s_q16CompainsRegex = new Regex(".*Customer.*Complaints.*", RegexOptions.Compiled);
        private static readonly Regex s_q16NumbersRegex = new Regex("^(49|14|23|45|19|3|36|9)$", RegexOptions.Compiled);
        internal void Q16()
        {
            Func<Column, Column> complains = Udf<string, bool>((x) => s_q16CompainsRegex.Match(x).Success);

            Func<Column, Column> polished = Udf<string, bool>((x) => x.StartsWith("MEDIUM POLISHED"));

            Func<Column, Column> numbers = Udf<int, bool>((x) => s_q16NumbersRegex.Match(x.ToString()).Success);

            DataFrame fparts = _part.Filter((Col("p_brand") != "Brand#45") & !polished(Col("p_type")) &
              numbers(Col("p_size")))
              .Select(Col("p_partkey"), Col("p_brand"), Col("p_type"), Col("p_size"));

            _supplier.Filter(!complains(Col("s_comment")))
              .Join(_partsupp, Col("s_suppkey") == _partsupp["ps_suppkey"])
              .Select(Col("ps_partkey"), Col("ps_suppkey"))
              .Join(fparts, Col("ps_partkey") == fparts["p_partkey"])
              .GroupBy(Col("p_brand"), Col("p_type"), Col("p_size"))
              .Agg(CountDistinct(Col("ps_suppkey")).As("supplier_count"))
              .Sort(Col("supplier_count").Desc(), Col("p_brand"), Col("p_type"), Col("p_size"))
              .Show();
        }

        internal void Q17()
        {
            Func<Column, Column> mul02 = Udf<double, double>((x) => x * 0.2);

            DataFrame flineitem = _lineitem.Select(Col("l_partkey"), Col("l_quantity"), Col("l_extendedprice"));

            DataFrame fpart = _part.Filter(Col("p_brand") == "Brand#23" & Col("p_container") == "MED BOX")
              .Select(Col("p_partkey"))
              .Join(_lineitem, Col("p_partkey") == _lineitem["l_partkey"], "left_outer");

            fpart.GroupBy("p_partkey")
              .Agg(mul02(Avg(Col("l_quantity"))).As("avg_quantity"))
              .Select(Col("p_partkey").As("key"), Col("avg_quantity"))
              .Join(fpart, Col("key") == fpart["p_partkey"])
              .Filter(Col("l_quantity") < Col("avg_quantity"))
              .Agg((Sum(Col("l_extendedprice")) / 7.0).As("avg_yearly"))
              .Show();
        }

        internal void Q18()
        {
            _lineitem.GroupBy(Col("l_orderkey"))
                    .Agg(Sum(Col("l_quantity")).As("sum_quantity"))
                    .Filter(Col("sum_quantity") > 300)
                    .Select(Col("l_orderkey").As("key"), Col("sum_quantity"))
                    .Join(_orders, _orders["o_orderkey"] == Col("key"))
                    .Join(_lineitem, Col("o_orderkey") == _lineitem["l_orderkey"])
                    .Join(_customer, _customer["c_custkey"] == Col("o_custkey"))
                    .Select(Col("l_quantity"), Col("c_name"), Col("c_custkey"), Col("o_orderkey"), Col("o_orderdate"), Col("o_totalprice"))
                    .GroupBy(Col("c_name"), Col("c_custkey"), Col("o_orderkey"), Col("o_orderdate"), Col("o_totalprice"))
                    .Agg(Sum("l_quantity"))
                    .Sort(Col("o_totalprice").Desc(), Col("o_orderdate"))
                    .Show();
        }

        private static readonly Regex s_q19SmRegex = new Regex("SM CASE|SM BOX|SM PACK|SM PKG", RegexOptions.Compiled);
        private static readonly Regex s_q19MdRegex = new Regex("MED BAG|MED BOX|MED PKG|MED PACK", RegexOptions.Compiled);
        private static readonly Regex s_q19LgRegex = new Regex("LG CASE|LG BOX|LG PACK|LG PKG", RegexOptions.Compiled);
        internal void Q19()
        {
            Func<Column, Column> sm = Udf<string, bool>(x => s_q19SmRegex.IsMatch(x));
            Func<Column, Column> md = Udf<string, bool>(x => s_q19MdRegex.IsMatch(x));
            Func<Column, Column> lg = Udf<string, bool>(x => s_q19LgRegex.IsMatch(x));

            Func<Column, Column, Column> decrease = Udf<double, double, double>((x, y) => x * (1 - y));

            _part.Join(_lineitem, Col("l_partkey") == Col("p_partkey"))
              .Filter((Col("l_shipmode") == "AIR" | Col("l_shipmode") == "AIR REG") &
                Col("l_shipinstruct") == "DELIVER IN PERSON")
              .Filter(
                ((Col("p_brand") == "Brand#12") &
                  sm(Col("p_container")) &
                  Col("l_quantity") >= 1 & Col("l_quantity") <= 11 &
                  Col("p_size") >= 1 & Col("p_size") <= 5) |
                  ((Col("p_brand") == "Brand#23") &
                    md(Col("p_container")) &
                    Col("l_quantity") >= 10 & Col("l_quantity") <= 20 &
                    Col("p_size") >= 1 & Col("p_size") <= 10) |
                  ((Col("p_brand") == "Brand#34") &
                    lg(Col("p_container")) &
                    Col("l_quantity") >= 20 & Col("l_quantity") <= 30 &
                    Col("p_size") >= 1 & Col("p_size") <= 15))
              .Select(decrease(Col("l_extendedprice"), Col("l_discount")).As("volume"))
              .Agg(Sum("volume").As("revenue"))
              .Show();
        }

        internal void Q20()
        {
            Func<Column, Column> forest = Udf<string, bool>(x => x.StartsWith("forest"));

            DataFrame flineitem = _lineitem.Filter(Col("l_shipdate") >= "1994-01-01" & Col("l_shipdate") < "1995-01-01")
              .GroupBy(Col("l_partkey"), Col("l_suppkey"))
              .Agg((Sum(Col("l_quantity")) * 0.5).As("sum_quantity"));

            DataFrame fnation = _nation.Filter(Col("n_name") == "CANADA");
            DataFrame nat_supp = _supplier.Select(Col("s_suppkey"), Col("s_name"), Col("s_nationkey"), Col("s_address"))
              .Join(fnation, Col("s_nationkey") == fnation["n_nationkey"]);

            _part.Filter(forest(Col("p_name")))
              .Select(Col("p_partkey"))
              .Distinct()
              .Join(_partsupp, Col("p_partkey") == _partsupp["ps_partkey"])
              .Join(flineitem, Col("ps_suppkey") == flineitem["l_suppkey"] & Col("ps_partkey") == flineitem["l_partkey"])
              .Filter(Col("ps_availqty") > Col("sum_quantity"))
              .Select(Col("ps_suppkey"))
              .Distinct()
              .Join(nat_supp, Col("ps_suppkey") == nat_supp["s_suppkey"])
              .Select(Col("s_name"), Col("s_address"))
              .Sort(Col("s_name"))
              .Show();
        }

        internal void Q21()
        {
            DataFrame fsupplier = _supplier.Select(Col("s_suppkey"), Col("s_nationkey"), Col("s_name"));

            DataFrame plineitem = _lineitem
                            .Select(Col("l_suppkey"), Col("l_orderkey"), Col("l_receiptdate"), Col("l_commitdate"));

            DataFrame flineitem = plineitem.Filter(Col("l_receiptdate") > Col("l_commitdate"));

            DataFrame line1 = plineitem.GroupBy(Col("l_orderkey"))
              .Agg(CountDistinct(Col("l_suppkey")).As("suppkey_count"), Max(Col("l_suppkey")).As("suppkey_max"))
              .Select(Col("l_orderkey").As("key"), Col("suppkey_count"), Col("suppkey_max"));

            DataFrame line2 = flineitem.GroupBy(Col("l_orderkey"))
              .Agg(CountDistinct(Col("l_suppkey")).As("suppkey_count"), Max(Col("l_suppkey")).As("suppkey_max"))
              .Select(Col("l_orderkey").As("key"), Col("suppkey_count"), Col("suppkey_max"));

            DataFrame forder = _orders.Select(Col("o_orderkey"), Col("o_orderstatus"))
              .Filter(Col("o_orderstatus") == "F");

            _nation.Filter(Col("n_name") == "SAUDI ARABIA")
              .Join(fsupplier, Col("n_nationkey") == fsupplier["s_nationkey"])
              .Join(flineitem, Col("s_suppkey") == flineitem["l_suppkey"])
              .Join(forder, Col("l_orderkey") == forder["o_orderkey"])
              .Join(line1, Col("l_orderkey") == line1["key"])
              .Filter(Col("suppkey_count") > 1)
              .Select(Col("s_name"), Col("l_orderkey"), Col("l_suppkey"))
              .Join(line2, Col("l_orderkey") == line2["key"], "left_outer")
              .Select(Col("s_name"), Col("l_orderkey"), Col("l_suppkey"), Col("suppkey_count"), Col("suppkey_max"))
              .Filter(Col("suppkey_count") == 1 & Col("l_suppkey") == Col("suppkey_max"))
              .GroupBy(Col("s_name"))
              .Agg(Count(Col("l_suppkey")).As("numwait"))
              .Sort(Col("numwait").Desc(), Col("s_name"))
              .Show();
        }

        private static readonly Regex s_q22PhoneRegex = new Regex("^(13|31|23|29|30|18|17)$", RegexOptions.Compiled);
        internal void Q22()
        {
            Func<Column, Column> sub2 = Udf<string, string>(x => x.Substring(0, 2));

            Func<Column, Column> phone = Udf<string, bool>(x => s_q22PhoneRegex.IsMatch(x));

            DataFrame fcustomer = _customer.Select(Col("c_acctbal"), Col("c_custkey"), sub2(Col("c_phone")).As("cntrycode"))
              .Filter(phone(Col("cntrycode")));

            DataFrame avg_customer = fcustomer.Filter(Col("c_acctbal") > 0.0)
              .Agg(Avg(Col("c_acctbal")).As("avg_acctbal"));

            _orders.GroupBy(Col("o_custkey"))
              .Agg(Col("o_custkey")).Select(Col("o_custkey"))
              .Join(fcustomer, Col("o_custkey") == fcustomer["c_custkey"], "right_outer")
              .Filter(Col("o_custkey").IsNull())
              .Join(avg_customer)
              .Filter(Col("c_acctbal") > Col("avg_acctbal"))
              .GroupBy(Col("cntrycode"))
              .Agg(Count(Col("c_acctbal")).As("numcust"), Sum(Col("c_acctbal")).As("totacctbal"))
              .Sort(Col("cntrycode"))
              .Show();
        }
    }
}
