// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.E2ETest;
using Xunit;

namespace Microsoft.Spark.Extensions.Hyperspace.E2ETest
{
    public class HyperspaceFixture
    {
        public HyperspaceFixture()
        {
            Version sparkVersion = SparkSettings.Version;
            string hyperspaceVersion = sparkVersion.Major switch
            {
                2 => "hyperspace-core_2.11:0.4.0",
                3 => "hyperspace-core_2.12:0.4.0",
                _ => throw new NotSupportedException($"Spark {sparkVersion} not supported.")
            };

            Environment.SetEnvironmentVariable(
                SparkFixture.EnvironmentVariableNames.ExtraSparkSubmitArgs,
                $"--packages com.microsoft.hyperspace:{hyperspaceVersion}");

            SparkFixture = new SparkFixture();
        }

        public SparkFixture SparkFixture { get; private set; }
    }

    [CollectionDefinition(Constants.HyperspaceTestContainerName)]
    public class HyperspaceTestCollection : ICollectionFixture<HyperspaceFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}
