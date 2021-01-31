// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.E2ETest;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Extensions.Hyperspace.Index;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.Extensions.Hyperspace.E2ETest
{
    /// <summary>
    /// Test suite for Hyperspace index management APIs.
    /// </summary>
    [Collection(Constants.HyperspaceTestContainerName)]
    public class HyperspaceTests : IDisposable
    {
        private readonly SparkSession _spark;
        private readonly TemporaryDirectory _hyperspaceSystemDirectory;
        private readonly Hyperspace _hyperspace;

        // Fields needed for sample DataFrame.
        private readonly DataFrame _sampleDataFrame;
        private readonly string _sampleIndexName;
        private readonly IndexConfig _sampleIndexConfig;

        public HyperspaceTests(HyperspaceFixture fixture)
        {
            _spark = fixture.SparkFixture.Spark;
            _hyperspaceSystemDirectory = new TemporaryDirectory();
            _spark.Conf().Set("spark.hyperspace.system.path", _hyperspaceSystemDirectory.Path);
            _hyperspace = new Hyperspace(_spark);

            _sampleDataFrame = _spark.Read()
                .Option("header", true)
                .Option("delimiter", ";")
                .Csv($"{TestEnvironment.ResourceDirectory}people.csv");
            _sampleIndexName = "sample_dataframe";
            _sampleIndexConfig = new IndexConfig(_sampleIndexName, new[] { "job" }, new[] { "name" });
            _hyperspace.CreateIndex(_sampleDataFrame, _sampleIndexConfig);
        }

        /// <summary>
        /// Clean up the Hyperspace system directory in between tests.
        /// </summary>
        public void Dispose()
        {
            _hyperspaceSystemDirectory.Dispose();
        }

        /// <summary>
        /// Test the method signatures for all Hyperspace APIs.
        /// </summary>
        [SkipIfSparkVersionIsNotInRange(Versions.V2_4_0, Versions.V3_0_0)]
        public void TestSignatures()
        {
            // Indexes API.
            Assert.IsType<DataFrame>(_hyperspace.Indexes());

            // Delete and Restore APIs.
            _hyperspace.DeleteIndex(_sampleIndexName);
            _hyperspace.RestoreIndex(_sampleIndexName);

            // Refresh API.
            _hyperspace.RefreshIndex(_sampleIndexName);
            _hyperspace.RefreshIndex(_sampleIndexName, "incremental");

            // Optimize API.
            _hyperspace.OptimizeIndex(_sampleIndexName);
            _hyperspace.OptimizeIndex(_sampleIndexName, "quick");

            // Index metadata API.
            Assert.IsType<DataFrame>(_hyperspace.Index(_sampleIndexName));

            // Cancel API.
            Assert.Throws<Exception>(() => _hyperspace.Cancel(_sampleIndexName));

            // Explain API.
            _hyperspace.Explain(_sampleDataFrame, true);
            _hyperspace.Explain(_sampleDataFrame, true, s => Console.WriteLine(s));

            // Delete and Vacuum APIs.
            _hyperspace.DeleteIndex(_sampleIndexName);
            _hyperspace.VacuumIndex(_sampleIndexName);

            // Enable and disable Hyperspace.
            Assert.IsType<SparkSession>(_spark.EnableHyperspace());
            Assert.IsType<SparkSession>(_spark.DisableHyperspace());
            Assert.IsType<bool>(_spark.IsHyperspaceEnabled());
        }

        /// <summary>
        /// Test E2E functionality of index CRUD APIs.
        /// </summary>
        [SkipIfSparkVersionIsNotInRange(Versions.V2_4_0, Versions.V3_0_0)]
        public void TestIndexCreateAndDelete()
        {
            // Should be one active index.
            DataFrame indexes = _hyperspace.Indexes();
            Assert.Equal(1, indexes.Count());
            Assert.Equal(_sampleIndexName, indexes.SelectExpr("name").First()[0]);
            Assert.Equal(States.Active, indexes.SelectExpr("state").First()[0]);

            // Delete the index then verify it has been deleted.
            _hyperspace.DeleteIndex(_sampleIndexName);
            indexes = _hyperspace.Indexes();
            Assert.Equal(1, indexes.Count());
            Assert.Equal(States.Deleted, indexes.SelectExpr("state").First()[0]);

            // Restore the index to active state and verify it is back.
            _hyperspace.RestoreIndex(_sampleIndexName);
            indexes = _hyperspace.Indexes();
            Assert.Equal(1, indexes.Count());
            Assert.Equal(States.Active, indexes.SelectExpr("state").First()[0]);

            // Delete and vacuum the index, then verify it is gone.
            _hyperspace.DeleteIndex(_sampleIndexName);
            _hyperspace.VacuumIndex(_sampleIndexName);
            Assert.Equal(0, _hyperspace.Indexes().Count());
        }

        /// <summary>
        /// Test that the explain API generates the expected string.
        /// </summary>
        [SkipIfSparkVersionIsNotInRange(Versions.V2_4_0, Versions.V3_0_0)]
        public void TestExplainAPI()
        {
            // Run a query that hits the index.
            DataFrame queryDataFrame = _sampleDataFrame
                .Where("job == 'Developer'")
                .Select("name");

            string explainString = string.Empty;
            _hyperspace.Explain(queryDataFrame, true, s => explainString = s);
            Assert.False(string.IsNullOrEmpty(explainString));
        }

        /// <summary>
        /// Index states used in testing.
        /// </summary>
        private static class States
        {
            public const string Active = "ACTIVE";
            public const string Deleted = "DELETED";
        }
    }
}
