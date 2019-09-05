// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Extensions.Delta.Tables
{
    /// <summary>
    /// Main class for programmatically interacting with Delta tables.
    /// You can create DeltaTable instances using the static methods.
    /// 
    /// <code>
    /// DeltaTable.ForPath(sparkSession, pathToTheDeltaTable)
    /// </code>
    /// </summary>
    public class DeltaTable : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        internal DeltaTable(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        /// <summary>
        /// Create a DeltaTable for the data at the given <c>path</c>.
        /// 
        /// Note: This uses the active SparkSession in the current thread to read the table data.
        /// Hence, this throws error if active SparkSession has not been set, that is,
        /// <c>SparkSession.GetActiveSession()</c> is empty.
        /// </summary>
        /// <param name="path"></param>
        /// <returns>DeltaTable loaded from the path.</returns>
        public static DeltaTable ForPath(string path) =>
            new DeltaTable((JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "io.delta.tables.DeltaTable",
                "forPath",
                path));

        /// <summary>
        /// Create a DeltaTable for the data at the given <c>path</c> using the given SparkSession
        /// to read the data.
        /// </summary>
        /// <param name="sparkSession"></param>
        /// <param name="path"></param>
        /// <returns>DeltaTable loaded from the path.</returns>
        public static DeltaTable ForPath(SparkSession sparkSession, string path) =>
            new DeltaTable((JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "io.delta.tables.DeltaTable",
                "forPath",
                ((IJvmObjectReferenceProvider)sparkSession).Reference,
                path));

        /// <summary>
        /// Apply an alias to the DeltaTable. This is similar to <c>Dataset.As(alias)</c> or SQL
        /// <c>tableName AS alias</c>.
        /// </summary>
        /// <param name="alias"></param>
        /// <returns>Aliased DeltaTable.</returns>
        public DeltaTable As(string alias) =>
            new DeltaTable((JvmObjectReference)_jvmObject.Invoke("as", alias));

        /// <summary>
        /// Get a DataFrame (that is, Dataset[Row]) representation of this Delta table.
        /// </summary>
        /// <returns>DataFrame representation of Delta table.</returns>
        public DataFrame ToDF() => new DataFrame((JvmObjectReference)_jvmObject.Invoke("toDF"));

        /// <summary>
        /// Recursively delete files and directories in the table that are not needed by the table
        /// for maintaining older versions up to the given retention threshold. This method will
        /// return an empty DataFrame on successful completion.
        /// </summary>
        /// <param name="retentionHours">The retention threshold in hours. Files required by the
        /// table for reading versions earlier than this will be preserved and the rest of them
        /// will be deleted.</param>
        /// <returns>Vacuumed DataFrame.</returns>
        public DataFrame Vacuum(double retentionHours) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("vacuum", retentionHours));

        /// <summary>
        /// Recursively delete files and directories in the table that are not needed by the table
        /// for maintaining older versions up to the given retention threshold. This method will
        /// return an empty DataFrame on successful completion.
        /// 
        /// Note: This will use the default retention period of 7 hours.
        /// </summary>
        /// <returns>Vacuumed DataFrame.</returns>
        public DataFrame Vacuum() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("vacuum"));

        /// <summary>
        /// Get the information of the latest <c>limit</c> commits on this table as a Spark
        /// DataFrame. The information is in reverse chronological order.
        /// </summary>
        /// <param name="limit">The number of previous commands to get history for.</param>
        /// <returns>History DataFrame.</returns>
        public DataFrame History(int limit) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("history", limit));

        /// <summary>
        /// Get the information available commits on this table as a Spark DataFrame. The
        /// information is in reverse chronological order.
        /// </summary>
        /// <returns>History DataFrame</returns>
        public DataFrame History() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("history"));

        /// <summary>
        /// Delete data from the table that match the given <c>condition</c>.
        /// </summary>
        /// <param name="condition">Boolean SQL expression.</param>
        public void Delete(string condition) => _jvmObject.Invoke("delete", condition);

        /// <summary>
        /// Delete data from the table that match the given <c>condition</c>.
        /// </summary>
        /// <param name="condition">Boolean SQL expression.</param>
        public void Delete(Column condition) =>
            _jvmObject.Invoke("delete", ((IJvmObjectReferenceProvider)condition).Reference);

        /// <summary>
        /// Delete data from the table.
        /// </summary>
        public void Delete() => _jvmObject.Invoke("delete");

        /// <summary>
        /// Update rows in the table based on the rules defined by <c>set</c>.
        /// </summary>
        /// <example>
        /// Example to increment the column <c>data</c>.
        /// <code>
        /// deltaTable.Update(new Dictionary<string, Column>(){
        ///     {"data" , table.Col("data").Plus(1) }   
        /// })
        /// </code>
        /// </example>
        /// <param name="set">Pules to update a row as a Scala map between target column names
        /// and corresponding update expressions as Column objects.</param>
        public void Update(Dictionary<string, Column> set) => _jvmObject.Invoke("update", set);

        /// <summary>
        /// Update data from the table on the rows that match the given <c>condition</c> based on
        /// the rules defined by <c>set</c>.
        /// </summary>
        /// <example>
        /// Example to increment the column <c>data</c>.
        /// <code>
        /// deltaTable.Update(
        ///     table.Col("date").Gt("2018-01-01")
        ///     new Dictionary<string, Column>(){
        ///         {"data" , table.Col("data").Plus(1) }   
        ///     })
        /// </code>
        /// </example>
        /// <param name="condition">Boolean expression as Column object specifying which rows
        /// to update.</param>
        /// <param name="set">Rules to update a row as a Scala map between target column names and
        /// corresponding update expressions as Column objects.</param>
        public void Update(Column condition, Dictionary<string, Column> set) =>
            _jvmObject.Invoke("update", ((IJvmObjectReferenceProvider)condition).Reference, set);

        /// <summary>
        /// Update rows in the table based on the rules defined by <c>set</c>.
        /// </summary>
        /// <example>
        /// Example to increment the column <c>data</c>.
        /// <code>
        /// deltaTable.UpdateExpr(
        ///     new Dictionary<string, string>(){
        ///         {"data" , "data + 1" }   
        ///     })
        /// </code>
        /// </example>
        /// <param name="set">Rules to update a row as a Scala map between target column names and
        /// corresponding update expressions as SQL formatted strings.</param>
        public void UpdateExpr(Dictionary<string, string> set) =>
            _jvmObject.Invoke("updateExpr", set);

        /// <summary>
        /// Update data from the table on the rows that match the given <c>condition</c>, which
        /// performs the rules defined by <c>set</c>.
        /// </summary>
        /// <example>
        /// Example to increment the column <c>data</c>.
        /// <code>
        /// deltaTable.UpdateExpr(
        ///     "date > '2018-01-01'",
        ///     new Dictionary<string, string>(){
        ///         {"data" , "data + 1" }   
        /// })
        /// </code>
        /// </example>
        /// <param name="condition">Boolean expression as SQL formatted string object specifying 
        /// which rows to update.</param>
        /// <param name="set">Rules to update a row as a map between target column names and
        /// corresponding update expressions as SQL formatted strings.</param>
        public void UpdateExpr(string condition, Dictionary<string, string> set) =>
            _jvmObject.Invoke("updateExpr", condition, set);

        /// <summary>
        /// Merge data from the <c>source</c> DataFrame based on the given merge <c>condition</c>.
        /// This class returns a <c>DeltaMergeBuilder</c> object that can be used to specify the
        /// update, delete, or insert actions to be performed on rows based on whether the rows
        /// matched the condition or not.
        ///
        /// See the <see cref="DeltaMergeBuilder"/> for a full description of this operation and
        /// what combination update, delete and insert operations are allowed.
        /// </summary>
        /// <example>
        /// See the <c>DeltaMergeBuilder</c> for a full description of this operation and what combination
        /// update, delete and insert operations are allowed.
        ///
        /// Example to update a key-value Delta table with new key-values from a source DataFrame:
        /// <code>
        ///    deltaTable
        ///     .As("target")
        ///     .Merge(
        ///       source.As("source"),
        ///       "target.key = source.key")
        ///     .WhenMatched
        ///     .UpdateExpr(
        ///        new Dictionary<String, String>() {
        ///          {"value", "source.value"}
        ///        })
        ///     .WhenNotMatched()
        ///     .InsertExpr(
        ///        new Dictionary<String, String>() {
        ///         {"key", "source.key"};
        ///         {"value", "source.value"};
        ///       })
        ///     .Execute();
        /// </code>
        /// </example>
        /// <param name="source">Source Dataframe to be merged.</param>
        /// <param name="condition">Boolean expression as SQL formatted string.</param>
        /// <returns>DeltaMergeBuilder</returns>
        public DeltaMergeBuilder Merge(DataFrame source, string condition) =>
            new DeltaMergeBuilder((JvmObjectReference)_jvmObject.Invoke(
                "merge",
                ((IJvmObjectReferenceProvider)source).Reference,
                condition));

        /// <summary>
        /// Merge data from the <c>source</c> DataFrame based on the given merge <c>condition</c>.
        /// This class returns a <c>DeltaMergeBuilder</c> object that can be used to specify the
        /// update, delete, or insert actions to be performed on rows based on whether the rows
        /// matched the condition or not.
        ///
        /// See the <see cref="DeltaMergeBuilder"/> for a full description of this operation and
        /// what combination update, delete and insert operations are allowed.
        /// </summary>
        /// <example>
        /// Example to update a key-value Delta table with new key-values from a source DataFrame:
        /// <code>
        ///    deltaTable
        ///     .As("target")
        ///     .Merge(
        ///       source.As("source"),
        ///       "target.key = source.key")
        ///     .WhenMatched()
        ///     .UpdateExpr(
        ///        new Dictionary<String, String>() {
        ///          {"value", "source.value"}
        ///        })
        ///     .WhenNotMatched()
        ///     .InsertExpr(
        ///        new Dictionary<String, String>() {
        ///         {"key", "source.key"},
        ///         {"value", "source.value"}
        ///       })
        ///     .Execute()
        /// </code>
        /// </example>
        /// <param name="source">Source Dataframe to be merged.</param>
        /// <param name="condition">Coolean expression as a Column object</param>
        /// <returns>DeltaMergeBuilder</returns>
        public DeltaMergeBuilder Merge(DataFrame source, Column condition) =>
            new DeltaMergeBuilder((JvmObjectReference)_jvmObject.Invoke(
                "merge",
                ((IJvmObjectReferenceProvider)source).Reference,
                ((IJvmObjectReferenceProvider)condition).Reference));
    }
}
