// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.E2ETest.Utils
{
    internal static class SQLUtils
    {
        /// <summary>
        /// Drops tables in <paramref name="tableNames"/> after calling <paramref name="action"/>.
        /// </summary>
        /// <param name="spark">The <see cref="SparkSession"/></param>
        /// <param name="tableNames">Names of the tables to drop</param>
        /// <param name="action"><see cref="Action"/> to execute.</param>
        public static void WithTable(SparkSession spark, IEnumerable<string> tableNames, Action action)
        {
            try
            {
                action();
            }
            finally
            {
                tableNames.ToList().ForEach(name => spark.Sql($"DROP TABLE IF EXISTS {name}"));
            }
        }
    }
}
