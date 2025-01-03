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
            Version sparkVersion = SparkSettings.Version;
            string deltaVersion = (sparkVersion.Major, sparkVersion.Minor, sparkVersion.Build) switch
            {
                (2, _, _) => "delta-core_2.11:0.6.1",
                (3, 0, _) => "delta-core_2.12:0.8.0",
                (3, 1, _) => "delta-core_2.12:1.0.0",
                (3, 2, _) => "delta-core_2.12:1.1.0",
                (3, 3, 0) => "delta-core_2.12:2.1.0",
                (3, 3, 1) => "delta-core_2.12:2.1.0",
                (3, 3, 2) => "delta-core_2.12:2.3.0",
                (3, 3, 3) => "delta-core_2.12:2.3.0",
                (3, 3, 4) => "delta-core_2.12:2.3.0",
                (3, 5, _) => "delta-spark_2.12:3.2.0",
                _ => throw new NotSupportedException($"Spark {sparkVersion} not supported.")
            };

            (string, string)[] conf = new[]
            {
                ("spark.databricks.delta.snapshotPartitions", "2"),
                ("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5"),
                // Set the writer protocol version for testing UpgradeTableProtocol().
                ("spark.databricks.delta.minWriterVersion", "2")
            };

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

            string confStr =
                string.Join(" ", conf.Concat(extraConf).Select(c => $"--conf {c.Item1}={c.Item2}"));

            Environment.SetEnvironmentVariable(
                SparkFixture.EnvironmentVariableNames.ExtraSparkSubmitArgs,
                $"--packages io.delta:{deltaVersion} {confStr}");
            SparkFixture = new SparkFixture();
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
