# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.
# See the LICENSE file in the project root for more information.

import pyspark
from pyspark.sql import SparkSession


class TpchBase:
    def __init__(self, spark, dir):
        self.customer = spark.read.parquet(dir + "customer")
        self.lineitem = spark.read.parquet(dir + "lineitem")
        self.nation = spark.read.parquet(dir + "nation")
        self.region = spark.read.parquet(dir + "region")
        self.orders = spark.read.parquet(dir + "orders")
        self.part = spark.read.parquet(dir + "part")
        self.partsupp = spark.read.parquet(dir + "partsupp")
        self.supplier = spark.read.parquet(dir + "supplier")
