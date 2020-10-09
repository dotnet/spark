// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Utils.AssemblyInfoProvider;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.Experimental.Sql
{
    public static class SparkSessionExtensions
    {
        /// <summary>
        /// Get the <see cref="AssemblyInfo"/> for the "Microsoft.Spark" assembly running
        /// on the Spark Driver and make a "best effort" attempt in determining the
        /// <see cref="AssemblyInfo"/> of "Microsoft.Spark.Worker"
        /// assembly on the Spark Executors.
        /// 
        /// There is no guarantee that a Spark Executor will be run on all the nodes in
        /// a cluster. To increase the likelyhood, the spark conf `spark.executor.instances`
        /// and the <paramref name="numPartitions"/> settings should be adjusted to a
        /// reasonable number relative to the number of nodes in the Spark cluster.
        /// </summary>
        /// <param name="session">The <see cref="SparkSession"/></param>
        /// <param name="numPartitions">Number of partitions</param>
        /// <returns>
        /// A <see cref="DataFrame"/> containing the <see cref="AssemblyInfo"/>
        /// </returns>
        public static DataFrame GetAssemblyInfo(this SparkSession session, int numPartitions = 10)
        {
            var schema = new StructType(new StructField[]
            {
                new StructField("AssemblyName", new StringType(), isNullable: false),
                new StructField("AssemblyVersion", new StringType(), isNullable: false),
                new StructField("HostName", new StringType(), isNullable: false)
            });

            DataFrame driverAssmeblyInfoDf = session.CreateDataFrame(
                new GenericRow[] { CreateGenericRow(MicrosoftSparkAssemblyInfo()) },
                schema);

            Func<Column, Column> executorAssemblyInfoUdf = Udf<int>(
                i => CreateGenericRow(MicrosoftSparkWorkerAssemblyInfo()),
                schema);
            DataFrame df = session.CreateDataFrame(Enumerable.Range(0, 10 * numPartitions));

            string tempColName = "ExecutorAssemblyInfo";
            DataFrame executorAssemblyInfoDf = df
                .Repartition(numPartitions)
                .WithColumn(tempColName, executorAssemblyInfoUdf(df["_1"]))
                .Select(schema.Fields.Select(f => Col($"{tempColName}.{f.Name}")).ToArray());

            return driverAssmeblyInfoDf
                .Union(executorAssemblyInfoDf)
                .DropDuplicates()
                .Sort(schema.Fields.Select(f => Col(f.Name)).ToArray());
        }

        private static GenericRow CreateGenericRow(AssemblyInfo assemblyInfo) =>
            new GenericRow(new object[]
            {
                assemblyInfo.AssemblyName,
                assemblyInfo.AssemblyVersion,
                assemblyInfo.HostName
            });
    }
}
