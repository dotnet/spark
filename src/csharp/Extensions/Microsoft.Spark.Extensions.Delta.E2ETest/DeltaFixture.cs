// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.E2ETest;

namespace Microsoft.Spark.Extensions.Delta.E2ETest
{
    public class DeltaFixture
    {
        public SparkFixture SparkFixture { get; private set; }

        public DeltaFixture()
        {
            // Set environment variables.
            Environment.SetEnvironmentVariable(
                Services.ConfigurationService.PackagesVarName,
                "io.delta:delta-core_2.11:0.3.0");

            SparkFixture = new SparkFixture();
        }
    }
}
