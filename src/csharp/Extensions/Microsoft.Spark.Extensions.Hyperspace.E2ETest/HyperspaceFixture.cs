﻿// Licensed to the .NET Foundation under one or more agreements.
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
            Environment.SetEnvironmentVariable(
                SparkFixture.EnvironmentVariableNames.ExtraSparkSubmitArgs,
                "--packages com.microsoft.hyperspace:hyperspace-core_2.11:0.1.0");

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
