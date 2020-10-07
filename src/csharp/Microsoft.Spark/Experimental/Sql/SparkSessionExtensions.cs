// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Experimental.Sql
{
    public static class SparkSessionExtensions
    {
        /// <summary>
        /// Get the <see cref="VersionSensor.VersionInfo"/> for the Microsoft.Spark assembly
        /// running on the Spark Driver and make a "best effort" attempt in determining the
        /// version of Microsoft.Spark.Worker assembly on the Spark Executors.
        /// 
        /// There is no guarantee that a Spark Executor will be run on all the nodes in
        /// a cluster. To increase the likelyhood, the spark conf `spark.executor.instances`
        /// and the <paramref name="numPartitions"/> settings should be a adjusted to a
        /// reasonable number relative to the number of nodes in the Spark cluster.
        /// </summary>
        /// <param name="session">The <see cref="SparkSession"/></param>
        /// <param name="numPartitions">Number of partitions</param>
        /// <returns>
        /// A <see cref="DataFrame"/> containing the <see cref="VersionSensor.VersionInfo"/>
        /// </returns>
        public static DataFrame Version(this SparkSession session, int numPartitions = 10)
        {
            StructType schema = VersionSensor.VersionInfo.s_schema;

            DataFrame sparkInfoDf = session.CreateDataFrame(
                new GenericRow[] { VersionSensor.MicrosoftSparkVersion().ToGenericRow() },
                schema);

            Func<Column, Column> workerInfoUdf = Functions.Udf<int>(
                i => VersionSensor.MicrosoftSparkWorkerVersion().ToGenericRow(),
                schema);
            DataFrame df = session.CreateDataFrame(Enumerable.Range(0, 10 * numPartitions));

            string tempColName = "WorkerVersionInfo";
            DataFrame workerInfoDf = df
                .Repartition(numPartitions)
                .WithColumn(tempColName, workerInfoUdf(df["_1"]))
                .Select(
                    schema.Fields.Select(f => Functions.Col($"{tempColName}.{f.Name}")).ToArray());

            return sparkInfoDf
                .Union(workerInfoDf)
                .DropDuplicates()
                .Sort(schema.Fields.Select(f => Functions.Col(f.Name)).ToArray());
        }
    }
}
