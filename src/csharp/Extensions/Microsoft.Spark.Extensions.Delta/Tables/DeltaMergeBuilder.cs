// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Extensions.Delta.Tables
{
    /// <summary>
    /// Builder to specify how to merge data from source DataFrame into the target Delta table.
    /// You can specify 1, 2 or 3 "when" clauses of which there can be at most 2 "WhenMatched" clauses
    /// and at most 1 "WhenNotMatched" clause. Here are the constraints on these clauses.
    ///
    ///   - "WhenMatched" clauses:
    ///
    ///     - There can be at most one "update" action and one "delete" action in "WhenMatched" clauses.
    ///
    ///     - Each "WhenMatched" clause can have an optional condition. However, if there are two
    ///       "WhenMatched" clauses, then the first one must have a condition.
    ///
    ///     - When there are two "WhenMatched" clauses and there are conditions (or the lack of)
    ///       such that a row matches both clauses, then the first clause/action is executed.
    ///       In other words, the order of the "WhenMatched" clauses matter.
    ///
    ///     - If none of the "WhenMatched" clauses match a source-target row pair that satisfy
    ///       the merge condition, then the target rows will not be updated or deleted.
    ///
    ///     - If you want to update all the columns of the target Delta table with the
    ///       corresponding column of the source DataFrame, then you can use the
    ///       "WhenMatched(...).UpdateAll()". This is equivalent to
    ///     <code>
    ///         WhenMatched(...).UpdateExpr(new Dictionary&lt;string, string&gt;() {
    ///           {"col1", "source.col1"},
    ///           {"col2", "source.col2"},
    ///           ...})
    ///     </code>
    ///
    ///   - "WhenNotMatched" clauses:
    ///
    ///     - This clause can have only an "Insert" action, which can have an optional condition.
    ///
    ///     - If the "WhenNotMatched" clause is not present or if it is present but the non-matching
    ///       source row does not satisfy the condition, then the source row is not Inserted.
    ///
    ///     - If you want to Insert all the columns of the target Delta table with the
    ///       corresponding column of the source DataFrame, then you can use
    ///       "WhenMatched(...).InsertAll()". This is equivalent to
    ///
    ///     <code>
    ///         WhenMatched(...).InsertExpr(new Dictionary&lt;string, string&gt;() {
    ///             {"col1", "source.col1"},
    ///             {"col2", "source.col2"}
    ///           ...})
    ///     </code>
    ///
    /// <example>
    /// C# example to update a key-value Delta table with new key-values from a source DataFrame:
    /// <code>
    ///    deltaTable
    ///     .As("target")
    ///     .Merge(
    ///       source.As("source"),
    ///       "target.Key = source.key")
    ///     .WhenMatched
    ///     .UpdateExpr(new Dictionary&lt;string, string&gt;() {
    ///         {"value", "source.value"}
    ///     })
    ///     .WhenNotMatched
    ///     .InsertExpr(new Dictionary&lt;string, string&gt;() {
    ///         {"key", "source.key"},
    ///         {"value, "source.value"}
    ///     })
    ///     .Execute();
    /// </code>
    /// </example>
    /// </summary>
    [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
    public class DeltaMergeBuilder : IJvmObjectReferenceProvider
    {
        internal DeltaMergeBuilder(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Build the actions to perform when the merge condition was matched. This returns
        /// DeltaMergeMatchedActionBuilder object which can be used to specify how to update or
        /// delete the matched target table row with the source row.
        /// </summary>
        /// <returns>DeltaMergeMatchedActionBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeMatchedActionBuilder WhenMatched() =>
            new DeltaMergeMatchedActionBuilder(
                (JvmObjectReference)Reference.Invoke("whenMatched"));

        /// <summary>
        /// Build the actions to perform when the merge condition was matched and the given
        /// condition is true. This returns DeltaMergeMatchedActionBuilder object which can be
        /// used to specify how to update or delete the matched target table row with the source
        /// row.
        /// </summary>
        /// <param name="condition">Boolean expression as a SQL formatted string.</param>
        /// <returns>DeltaMergeMatchedActionBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeMatchedActionBuilder WhenMatched(string condition) =>
            new DeltaMergeMatchedActionBuilder(
                (JvmObjectReference)Reference.Invoke("whenMatched", condition));

        /// <summary>
        /// Build the actions to perform when the merge condition was matched and the given
        /// condition is true. This returns a DeltaMergeMatchedActionBuilder object which can be
        /// used to specify how to update or delete the matched target table row with the source
        /// row.
        /// </summary>
        /// <param name="condition">Boolean expression as a Column object.</param>
        /// <returns>DeltaMergeMatchedActionBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeMatchedActionBuilder WhenMatched(Column condition) =>
            new DeltaMergeMatchedActionBuilder(
                (JvmObjectReference)Reference.Invoke("whenMatched", condition));

        /// <summary>
        /// Build the action to perform when the merge condition was not matched. This returns 
        /// DeltaMergeNotMatchedActionBuilder object which can be used to specify how to insert the
        /// new sourced row into the target table.
        /// </summary>
        /// <returns>DeltaMergeNotMatchedActionBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeNotMatchedActionBuilder WhenNotMatched() =>
            new DeltaMergeNotMatchedActionBuilder(
                (JvmObjectReference)Reference.Invoke("whenNotMatched"));

        /// <summary>
        /// Build the actions to perform when the merge condition was not matched and the given
        /// condition is true. This returns DeltaMergeMatchedActionBuilder object which can be
        /// used to specify how to insert the new sourced row into the target table.
        /// </summary>
        /// <param name="condition">Boolean expression as a SQL formatted string.</param>
        /// <returns>DeltaMergeNotMatchedActionBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeNotMatchedActionBuilder WhenNotMatched(string condition) =>
            new DeltaMergeNotMatchedActionBuilder(
                (JvmObjectReference)Reference.Invoke("whenNotMatched", condition));

        /// <summary>
        /// Build the actions to perform when the merge condition was not matched and the given
        /// condition is true. This returns DeltaMergeMatchedActionBuilder object which can be
        /// used to specify how to insert the new sourced row into the target table.
        /// </summary>
        /// <param name="condition">Boolean expression as a Column object.</param>
        /// <returns>DeltaMergeNotMatchedActionBuilder object.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeNotMatchedActionBuilder WhenNotMatched(Column condition) =>
            new DeltaMergeNotMatchedActionBuilder(
                (JvmObjectReference)Reference.Invoke("whenNotMatched", condition));

        /// <summary>
        /// Execute the merge operation based on the built matched and not matched actions.
        /// </summary>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public void Execute() => Reference.Invoke("execute");
    }
}
