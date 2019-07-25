# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

import sys
import time

from tpch_functional_queries import *
from tpch_sql_queries import *
from pyspark.sql import SparkSession


def main():
    if len(sys.argv) != 5:
        print("Usage:")
        print("\t<spark-submit> --master local tpch.py")
        print("\t\t<tpch_data_root_path> <query_number> <num_iterations> <true for SQL | false for functional>")
        exit(1)

    input_dir = sys.argv[1]
    query_number = sys.argv[2]
    num_iterations = int(sys.argv[3])
    is_sql = sys.argv[4].lower() == "true"

    spark = SparkSession.builder.appName('TPCH Benchmark for Python').getOrCreate()
    for iter in range(0, num_iterations):
        print("TPCH Starting iteration {0} with query #{1}".format(iter, query_number))

        start = startFunc = endFunc = time.time()
        if (is_sql == False):
            queries = TpchFunctionalQueries(spark, input_dir)
            startFunc = time.time()
            getattr(queries, "q" + query_number)()
            endFunc = time.time()
        else:
            queries = TpchSqlQueries(spark, input_dir)
            getattr(queries, "q" + query_number)()
        end = time.time()

        typeStr = "SQL" if is_sql else "Functional"
        print("TPCH_Result,Python,%s,%s,%d,%d,%d" % (
            typeStr, query_number, iter, (end - start) * 1000, (endFunc - startFunc) * 1000))

    spark.stop()


if __name__ == '__main__':
    main()
