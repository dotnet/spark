// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.E2ETest;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.Extensions.Hyperspace.E2ETest
{
    public class HyperspaceFixture : IDisposable
    {
        private readonly TemporaryDirectory _temporaryDirectory;

        public HyperspaceFixture()
        {
            // Save Hyperspace system data in a temporary directory so that 
            // indexes aren't persisted between runs.
            _temporaryDirectory = new TemporaryDirectory();

            Environment.SetEnvironmentVariable(
                SparkFixture.EnvironmentVariableNames.ExtraSparkSubmitArgs,
                "--packages com.microsoft.hyperspace:hyperspace-core_2.11:0.0.1 " +
                $"--conf spark.hyperspace.system.path={_temporaryDirectory.Path}");

            SparkFixture = new SparkFixture();
            Hyperspace = new Hyperspace(SparkFixture.Spark);
        }

        public void Dispose()
        {
            _temporaryDirectory.Dispose();
        }

        public Hyperspace Hyperspace { get; private set; }

        public SparkFixture SparkFixture { get; private set; }

        public string HyperspaceSystemPath => _temporaryDirectory.Path;
    }

    [CollectionDefinition(Constants.HyperspaceTestContainerName)]
    public class HyperspaceTestCollection : ICollectionFixture<HyperspaceFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}
