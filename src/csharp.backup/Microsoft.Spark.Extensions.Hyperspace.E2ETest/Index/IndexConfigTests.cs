// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Extensions.Hyperspace.Index;
using Xunit;

namespace Microsoft.Spark.Extensions.Hyperspace.E2ETest.Index
{
    /// <summary>
    /// Test suite for Hyperspace IndexConfig tests.
    /// </summary>
    [Collection(Constants.HyperspaceTestContainerName)]
    public class IndexConfigTests
    {
        /// <summary>
        /// Test the method signatures for IndexConfig and IndexConfigBuilder APIs.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestSignatures()
        {
            string indexName = "testIndexName";
            var indexConfig = new IndexConfig(indexName, new[] { "Id" }, new string[] { });
            Assert.IsType<string>(indexConfig.IndexName);
            Assert.IsType<List<string>>(indexConfig.IndexedColumns);
            Assert.IsType<List<string>>(indexConfig.IncludedColumns);
            Assert.IsType<Builder>(IndexConfig.Builder());
            Assert.IsType<bool>(indexConfig.Equals(indexConfig));
            Assert.IsType<int>(indexConfig.GetHashCode());
            Assert.IsType<string>(indexConfig.ToString());

            Builder builder = IndexConfig.Builder();
            Assert.IsType<Builder>(builder);
            Assert.IsType<Builder>(builder.IndexName("indexName"));
            Assert.IsType<Builder>(builder.IndexBy("indexed1", "indexed2"));
            Assert.IsType<Builder>(builder.Include("included1"));
            Assert.IsType<IndexConfig>(builder.Create());
        }

        /// <summary>
        /// Test creating an IndexConfig using its class constructor.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestIndexConfigConstructor()
        {
            string indexName = "indexName";
            string[] indexedColumns = { "idx1" };
            string[] includedColumns = { "inc1", "inc2", "inc3" };
            var config = new IndexConfig(indexName, indexedColumns, includedColumns);

            // Validate that the config was built correctly.
            Assert.Equal(indexName, config.IndexName);
            Assert.Equal(indexedColumns, config.IndexedColumns);
            Assert.Equal(includedColumns, config.IncludedColumns);
        }

        /// <summary>
        /// Test creating an IndexConfig using the builder pattern.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_4_0)]
        public void TestIndexConfigBuilder()
        {
            string indexName = "indexName";
            string[] indexedColumns = { "idx1" };
            string[] includedColumns = { "inc1", "inc2", "inc3" };

            Builder builder = IndexConfig.Builder();
            builder.IndexName(indexName);
            builder.Include(includedColumns[0], includedColumns[1], includedColumns[2]);
            builder.IndexBy(indexedColumns[0]);

            // Validate that the config was built correctly.
            IndexConfig config = builder.Create();
            Assert.Equal(indexName, config.IndexName);
            Assert.Equal(indexedColumns, config.IndexedColumns);
            Assert.Equal(includedColumns, config.IncludedColumns);
        }
    }
}
