// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.Avro
{
    public class AvroFixture
    {
        public AvroFixture()
        {
            Version sparkVersion = SparkSettings.Version;
            string avroVersion = sparkVersion.Major switch
            {
                2 => $"spark-avro_2.11:{sparkVersion}",
                3 => $"spark-avro_2.12:{sparkVersion}",
                _ => throw new NotSupportedException($"Spark {sparkVersion} not supported.")
            };
            Environment.SetEnvironmentVariable(
                SparkFixture.EnvironmentVariableNames.ExtraSparkSubmitArgs,
                $"--packages org.apache.spark:{avroVersion}");

            SparkFixture = new SparkFixture();
        }

        public SparkFixture SparkFixture { get; private set; }
    }

    [CollectionDefinition(Constants.AvroTestContainerName)]
    public class AvroFunctionsTestCollection : ICollectionFixture<AvroFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}
