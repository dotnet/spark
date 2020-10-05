﻿// Licensed to the .NET Foundation under one or more agreements.
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
        private readonly JvmObjectReference _jvmObject;

        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public Hyperspace(SparkSession spark)
        {
            _spark = spark;
            _jvmBridge = ((IJvmObjectReferenceProvider)spark).Reference.Jvm;
            _jvmObject = _jvmBridge.CallConstructor(s_hyperspaceClassName, spark);
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Collect all the index metadata.
        /// </summary>
        /// <returns>All index metadata as a <see cref="DataFrame"/>.</returns>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public DataFrame Indexes() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("indexes"));

        /// <summary>
        /// Create index.
        /// </summary>
        /// <param name="df">The DataFrame object to build index on.</param>
        /// <param name="indexConfig">The configuration of index to be created.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void CreateIndex(DataFrame df, IndexConfig indexConfig) =>
            _jvmObject.Invoke("createIndex", df, indexConfig);

        /// <summary>
        /// Soft deletes the index with given index name.
        /// </summary>
        /// <param name="indexName">The name of index to delete.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void DeleteIndex(string indexName) => _jvmObject.Invoke("deleteIndex", indexName);

        /// <summary>
        /// Restores index with given index name.
        /// </summary>
        /// <param name="indexName">Name of the index to restore.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void RestoreIndex(string indexName) => _jvmObject.Invoke("restoreIndex", indexName);

        /// <summary>
        /// Does hard delete of indexes marked as <c>DELETED</c>.
        /// </summary>
        /// <param name="indexName">Name of the index to restore.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void VacuumIndex(string indexName) => _jvmObject.Invoke("vacuumIndex", indexName);

        /// <summary>
        /// Update indexes for the latest version of the data.
        /// </summary>
        /// <param name="indexName">Name of the index to refresh.</param>
        [HyperspaceSince(HyperspaceVersions.V0_0_1)]
        public void RefreshIndex(string indexName) => _jvmObject.Invoke("refreshIndex", indexName);

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
        public void Cancel(string indexName) => _jvmObject.Invoke("cancel", indexName);

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
    }
}
