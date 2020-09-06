﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
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
            string deltaVersion = sparkVersion.Major switch
            {
                2 => "delta-core_2.11:0.6.1",
                3 => "delta-core_2.12:0.7.0",
                _ => throw new NotSupportedException($"Spark {sparkVersion} not supported.")
            };

            Environment.SetEnvironmentVariable(
                SparkFixture.EnvironmentVariableNames.ExtraSparkSubmitArgs,
                $"--packages io.delta:{deltaVersion} " +
                "--conf spark.databricks.delta.snapshotPartitions=2 " +
                "--conf spark.sql.sources.parallelPartitionDiscovery.parallelism=5");
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
