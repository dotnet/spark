// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Extensions.Hyperspace.Index;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Extensions.Hyperspace
{
    /// <summary>
    /// .Net for Spark binding for Hyperspace index management APIs.
    /// </summary>
    [HyperspaceSince(HyperspaceVersions.V0_0_1)]
    public class Hyperspace : IJvmObjectReferenceProvider
    {
        private static readonly string s_hyperspaceClassName =
            "com.microsoft.hyperspace.Hyperspace";
        private readonly SparkSession _spark;
        private readonly IJvmBridge _jvmBridge;

        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public Hyperspace(SparkSession spark)
        {
            _spark = spark;
            _jvmBridge = spark.Reference.Jvm;
            Reference = _jvmBridge.CallConstructor(s_hyperspaceClassName, spark);
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Collect all the index metadata.
        /// </summary>
        /// <returns>All index metadata as a <see cref="DataFrame"/>.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public DataFrame Indexes() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("indexes"));

        /// <summary>
        /// Create index.
        /// </summary>
        /// <param name="df">The DataFrame object to build index on.</param>
        /// <param name="indexConfig">The configuration of index to be created.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void CreateIndex(DataFrame df, IndexConfig indexConfig) =>
            Reference.Invoke("createIndex", df, indexConfig);

        /// <summary>
        /// Soft deletes the index with given index name.
        /// </summary>
        /// <param name="indexName">The name of index to delete.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void DeleteIndex(string indexName) => Reference.Invoke("deleteIndex", indexName);

        /// <summary>
        /// Restores index with given index name.
        /// </summary>
        /// <param name="indexName">Name of the index to restore.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void RestoreIndex(string indexName) => Reference.Invoke("restoreIndex", indexName);

        /// <summary>
        /// Does hard delete of indexes marked as <c>DELETED</c>.
        /// </summary>
        /// <param name="indexName">Name of the index to restore.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void VacuumIndex(string indexName) => Reference.Invoke("vacuumIndex", indexName);

        /// <summary>
        /// Update indexes for the latest version of the data.
        /// </summary>
        /// <param name="indexName">Name of the index to refresh.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void RefreshIndex(string indexName) => Reference.Invoke("refreshIndex", indexName);

        /// <summary>
        /// Update indexes for the latest version of the data. This API provides a few supported refresh
        /// modes as listed below.
        /// </summary>
        /// <param name="indexName">Name of the index to refresh.</param>
        /// <param name="mode">Refresh mode. Currently supported modes are <c>incremental</c> and
        /// <c>full</c>.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_3)]
        public void RefreshIndex(string indexName, string mode) =>
            Reference.Invoke("refreshIndex", indexName, mode);

        /// <summary>
        /// Optimize index by changing the underlying index data layout (e.g., compaction).
        ///
        /// Note: This API does NOT refresh (i.e. update) the index if the underlying data changes. It only
        /// rearranges the index data into a better layout, by compacting small index files. The index files
        /// larger than a threshold remain untouched to avoid rewriting large contents.
        /// 
        /// <c>quick</c> optimize mode is used by default.
        /// 
        /// Available modes:
        /// <c>quick</c> mode: This mode allows for fast optimization. Files smaller than a predefined
        /// threshold <c>spark.hyperspace.index.optimize.fileSizeThreshold</c> will be picked for compaction.
        ///
        /// <c>full</c> mode: This allows for slow but complete optimization. ALL index files are picked for
        /// compaction.
        /// </summary>
        /// <param name="indexName">Name of the index to optimize.</param>
        /// <param name="mode">Optimize mode <c>quick</c> or <c>full</c>.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_3)]
        public void OptimizeIndex(string indexName, string mode = "quick") =>
            Reference.Invoke("optimizeIndex", indexName, mode);

        /// <summary>
        /// Cancel api to bring back index from an inconsistent state to the last known stable
        /// state.
        /// 
        /// E.g. if index fails during creation, in <c>CREATING</c> state.
        /// The index will not allow any index modifying operations unless a cancel is called.
        /// 
        /// Note: Cancel from <c>VACUUMING</c> state will move it forward to <c>DOESNOTEXIST</c>
        /// state.
        /// 
        /// Note: If no previous stable state exists, cancel will move it to <c>DOESNOTEXIST</c>
        /// state.
        /// </summary>
        /// <param name="indexName">Name of the index to cancel.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void Cancel(string indexName) => Reference.Invoke("cancel", indexName);

        /// <summary>
        /// Explains how indexes will be applied to the given dataframe.
        /// </summary>
        /// <param name="df">dataFrame</param>
        /// <param name="verbose">Flag to enable verbose mode.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void Explain(DataFrame df, bool verbose) =>
            Explain(df, verbose, s => Console.WriteLine(s));

        /// <summary>
        /// Explains how indexes will be applied to the given dataframe.
        /// </summary>
        /// <param name="df">dataFrame</param>
        /// <param name="verbose">Flag to enable verbose mode.</param>
        /// <param name="redirectFunc">Function to redirect output of explain.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void Explain(DataFrame df, bool verbose, Action<string> redirectFunc)
        {
            var explainString = (string)_jvmBridge.CallStaticJavaMethod(
                "com.microsoft.hyperspace.index.plananalysis.PlanAnalyzer",
                "explainString",
                df,
                _spark,
                Indexes(),
                verbose);
            redirectFunc(explainString);
        }

        /// <summary>
        /// Get index metadata and detailed index statistics for a given index.
        /// </summary>
        /// <param name="indexName">Name of the index to get stats for.</param>
        /// <returns>Index metadata and statistics as a <see cref="DataFrame"/>.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_4)]
        public DataFrame Index(string indexName) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("index", indexName));
    }
}
