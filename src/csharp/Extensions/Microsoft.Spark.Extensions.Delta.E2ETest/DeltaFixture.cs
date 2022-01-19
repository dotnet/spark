// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using Microsoft.Spark.E2ETest;
using Xunit;

namespace Microsoft.Spark.Extensions.Delta.E2ETest
{
    public class DeltaFixture
    {
        public SparkFixture SparkFixture { get; private set; }

        public DeltaFixture()
        {
            Console.WriteLine("Here 1");
            Version sparkVersion = SparkSettings.Version;
            string deltaVersion = (sparkVersion.Major, sparkVersion.Minor) switch
            {
                (2, _) => "delta-core_2.11:0.6.1",
                (3, 0) => "delta-core_2.12:0.8.0",
                (3, 1) => "delta-core_2.12:0.8.0",
                (3, 2) => "delta-core_2.12:1.1.0",
                _ => throw new NotSupportedException($"Spark {sparkVersion} not supported.")
            };
            Console.WriteLine("Here 2");

            (string, string)[] conf = new[]
            {
                ("spark.databricks.delta.snapshotPartitions", "2"),
                ("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5"),
                // Set the writer protocol version for testing UpgradeTableProtocol().
                ("spark.databricks.delta.minWriterVersion", "2")
            };
            Console.WriteLine("Here 3");

            (string, string)[] extraConf = sparkVersion.Major switch
            {
                2 => Array.Empty<(string, string)>(),
                3 => new[]
                {
                    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
                    ("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                },
                _ => throw new NotSupportedException($"Spark {sparkVersion} not supported.")
            };
            Console.WriteLine("Here 4");

            string confStr =
                string.Join(" ", conf.Concat(extraConf).Select(c => $"--conf {c.Item1}={c.Item2}"));
            Console.WriteLine("Here 5");

            Environment.SetEnvironmentVariable(
                SparkFixture.EnvironmentVariableNames.ExtraSparkSubmitArgs,
                $"--packages io.delta:{deltaVersion} {confStr}");
            Console.WriteLine("Here 6");
            SparkFixture = new SparkFixture();
            Console.WriteLine("Here end");
        }
    }

    [CollectionDefinition(Constants.DeltaTestContainerName)]
    public class DeltaTestCollection : ICollectionFixture<DeltaFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}
