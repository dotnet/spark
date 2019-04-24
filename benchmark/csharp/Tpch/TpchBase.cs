// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.Spark.Sql;

namespace Tpch
{
    internal class TpchBase
    {
        protected readonly DataFrame _customer, _lineitem, _nation, _orders,
            _part, _partsupp, _region, _supplier;

        internal TpchBase(string tpchRoot, SparkSession spark)
        {
            // Load all the TPC-H tables.
            tpchRoot += Path.DirectorySeparatorChar;
            _customer = spark.Read().Parquet($"{tpchRoot}customer");
            _lineitem = spark.Read().Parquet($"{tpchRoot}lineitem");
            _nation = spark.Read().Parquet($"{tpchRoot}nation");
            _orders = spark.Read().Parquet($"{tpchRoot}orders");
            _part = spark.Read().Parquet($"{tpchRoot}part");
            _partsupp = spark.Read().Parquet($"{tpchRoot}partsupp");
            _region = spark.Read().Parquet($"{tpchRoot}region");
            _supplier = spark.Read().Parquet($"{tpchRoot}supplier");
        }
    }
}
