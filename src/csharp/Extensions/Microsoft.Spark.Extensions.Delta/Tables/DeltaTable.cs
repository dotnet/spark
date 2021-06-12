// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

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
    [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
    public class DeltaTable : IJvmObjectReferenceProvider
    {
        private static readonly string s_deltaTableClassName = "io.delta.tables.DeltaTable";

        internal DeltaTable(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Create a DeltaTable from the given parquet table and partition schema.
        /// Takes an existing parquet table and constructs a delta transaction log in the base path
        /// of that table.
        /// 
        /// Note: Any changes to the table during the conversion process may not result in a
        /// consistent state at the end of the conversion. Users should stop any changes to the
        /// table before the conversion is started.
        /// 
        /// An example usage would be
        /// <code>
        /// DeltaTable.ConvertToDelta(
        ///     spark,
        ///     "parquet.`/path`",
        ///     new StructType(new[]
        ///     {
        ///         new StructField("key1", new LongType()),
        ///         new StructField("key2", new StringType())
        ///     });
        /// </code>
        /// </summary>
        /// <param name="spark">The relevant session.</param>
        /// <param name="identifier">String used to identify the parquet table.</param>
        /// <param name="partitionSchema">StructType representing the partition schema.</param>
        /// <returns>The converted DeltaTable.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_4_0)]
        public static DeltaTable ConvertToDelta(
            SparkSession spark,
            string identifier,
            StructType partitionSchema) =>
            new DeltaTable(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_deltaTableClassName,
                "convertToDelta",
                spark,
                identifier,
                DataType.FromJson(SparkEnvironment.JvmBridge, partitionSchema.Json)));

        /// <summary>
        /// Create a DeltaTable from the given parquet table and partition schema.
        /// Takes an existing parquet table and constructs a delta transaction log in the base path
        /// of that table.
        ///
        /// Note: Any changes to the table during the conversion process may not result in a
        /// consistent state at the end of the conversion. Users should stop any changes to the
        /// table before the conversion is started.
        ///
        /// An example usage would be
        /// <code>
        /// DeltaTable.ConvertToDelta(spark, "parquet.`/path`", "key1 long, key2 string")
        /// </code>
        /// </summary>
        /// <param name="spark">The relevant session.</param>
        /// <param name="identifier">String used to identify the parquet table.</param>
        /// <param name="partitionSchema">String representing the partition schema.</param>
        /// <returns>The converted DeltaTable.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_4_0)]
        public static DeltaTable ConvertToDelta(
            SparkSession spark,
            string identifier,
            string partitionSchema) =>
            new DeltaTable(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_deltaTableClassName,
                    "convertToDelta",
                    spark,
                    identifier,
                    partitionSchema));

        /// <summary>
        /// Create a DeltaTable from the given parquet table. Takes an existing parquet table and
        /// constructs a delta transaction log in the base path of the table.
        ///
        /// Note: Any changes to the table during the conversion process may not result in a
        /// consistent state at the end of the conversion. Users should stop any changes to the
        /// table before the conversion is started.
        ///
        /// An example would be
        /// <code>
        /// DeltaTable.ConvertToDelta(spark, "parquet.`/path`")
        /// </code>
        /// </summary>
        /// <param name="spark">The relevant session.</param>
        /// <param name="identifier">String used to identify the parquet table.</param>
        /// <returns>The converted DeltaTable.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_4_0)]
        public static DeltaTable ConvertToDelta(SparkSession spark, string identifier) =>
            new DeltaTable(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_deltaTableClassName,
                    "convertToDelta",
                    spark,
                    identifier));

        /// <summary>
        /// Create a DeltaTable for the data at the given <c>path</c>.
        /// 
        /// Note: This uses the active SparkSession in the current thread to read the table data.
        /// Hence, this throws error if active SparkSession has not been set, that is,
        /// <c>SparkSession.GetActiveSession()</c> is empty.
        /// </summary>
        /// <param name="path">The path to the data.</param>
        /// <returns>DeltaTable loaded from the path.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public static DeltaTable ForPath(string path) =>
            new DeltaTable(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_deltaTableClassName,
                    "forPath",
                    path));

        /// <summary>
        /// Create a DeltaTable for the data at the given <c>path</c> using the given SparkSession
        /// to read the data.
        /// </summary>
        /// <param name="sparkSession">The active SparkSession.</param>
        /// <param name="path">The path to the data.</param>
        /// <returns>DeltaTable loaded from the path.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public static DeltaTable ForPath(SparkSession sparkSession, string path) =>
            new DeltaTable(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_deltaTableClassName,
                    "forPath",
                    sparkSession,
                    path));

        /// <summary>
        /// Create a <see cref="DeltaTable"/> using the given table or view name using the given
        /// <see cref="SparkSession"/>.
        ///
        /// Note: This uses the active <see cref="SparkSession"/> in the current thread to read the
        /// table data. Hence, this throws error if active <see cref="SparkSession"/> has not been
        /// set, that is, <c>SparkSession.GetActiveSession()</c> is empty.
        /// </summary>
        /// <param name="tableOrViewName">Name of table or view to use.</param>
        /// <returns></returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_7_0)]
        public static DeltaTable ForName(string tableOrViewName) =>
            new DeltaTable(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_deltaTableClassName,
                    "forName",
                    tableOrViewName));

        /// <summary>
        /// Create a <see cref="DeltaTable"/> using the given table or view name using the given
        /// <see cref="SparkSession"/>.
        /// </summary>
        /// <param name="sparkSession">The active SparkSession.</param>
        /// <param name="tableOrViewName">Name of table or view to use.</param>
        /// <returns></returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_7_0)]
        public static DeltaTable ForName(SparkSession sparkSession, string tableOrViewName) =>
            new DeltaTable(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    s_deltaTableClassName,
                    "forName",
                    sparkSession,
                    tableOrViewName));

        /// <summary>
        /// Check if the provided <c>identifier</c> string, in this case a file path,
        /// is the root of a Delta table using the given SparkSession.
        ///
        /// An example would be
        /// <code>
        ///   DeltaTable.IsDeltaTable(spark, "path/to/table")
        /// </code>
        /// </summary>
        /// <param name="sparkSession">The relevant session.</param>
        /// <param name="identifier">String that identifies the table, e.g. path to table.</param>
        /// <returns>True if the table is a DeltaTable.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_4_0)]
        public static bool IsDeltaTable(SparkSession sparkSession, string identifier) =>
            (bool)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_deltaTableClassName,
                "isDeltaTable",
                sparkSession,
                identifier);

        /// <summary>
        /// Check if the provided <c>identifier</c> string, in this case a file path,
        /// is the root of a Delta table.
        ///
        /// Note: This uses the active SparkSession in the current thread to search for the table.
        /// Hence, this throws error if active SparkSession has not been set, that is,
        /// <c>SparkSession.GetActiveSession()</c> is empty.
        ///
        /// An example would be
        /// <code>
        ///   DeltaTable.IsDeltaTable(spark, "/path/to/table")
        /// </code>
        /// </summary>
        /// <param name="identifier">String that identifies the table, e.g. path to table.</param>
        /// <returns>True if the table is a DeltaTable.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_4_0)]
        public static bool IsDeltaTable(string identifier) =>
            (bool)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                s_deltaTableClassName,
                "isDeltaTable",
                identifier);

        /// <summary>
        /// Apply an alias to the DeltaTable. This is similar to <c>Dataset.As(alias)</c> or SQL
        /// <c>tableName AS alias</c>.
        /// </summary>
        /// <param name="alias">The table alias.</param>
        /// <returns>Aliased DeltaTable.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaTable As(string alias) =>
            new DeltaTable((JvmObjectReference)Reference.Invoke("as", alias));

        /// <summary>
        /// Apply an alias to the DeltaTable. This is similar to <c>Dataset.as(alias)</c>
        /// or SQL <c>tableName AS alias</c>.
        /// </summary>
        /// <param name="alias">The table alias.</param>
        /// <returns>Aliased DeltaTable.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaTable Alias(string alias) =>
            new DeltaTable((JvmObjectReference)Reference.Invoke("alias", alias));

        /// <summary>
        /// Get a DataFrame (that is, Dataset[Row]) representation of this Delta table.
        /// </summary>
        /// <returns>DataFrame representation of Delta table.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DataFrame ToDF() => new DataFrame((JvmObjectReference)Reference.Invoke("toDF"));

        /// <summary>
        /// Recursively delete files and directories in the table that are not needed by the table
        /// for maintaining older versions up to the given retention threshold. This method will
        /// return an empty DataFrame on successful completion.
        /// </summary>
        /// <param name="retentionHours">The retention threshold in hours. Files required by the
        /// table for reading versions earlier than this will be preserved and the rest of them
        /// will be deleted.</param>
        /// <returns>Vacuumed DataFrame.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DataFrame Vacuum(double retentionHours) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("vacuum", retentionHours));

        /// <summary>
        /// Recursively delete files and directories in the table that are not needed by the table
        /// for maintaining older versions up to the given retention threshold. This method will
        /// return an empty DataFrame on successful completion.
        /// 
        /// Note: This will use the default retention period of 7 days.
        /// </summary>
        /// <returns>Vacuumed DataFrame.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DataFrame Vacuum() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("vacuum"));

        /// <summary>
        /// Get the information of the latest <c>limit</c> commits on this table as a Spark
        /// DataFrame. The information is in reverse chronological order.
        /// </summary>
        /// <param name="limit">The number of previous commands to get history for.</param>
        /// <returns>History DataFrame.</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DataFrame History(int limit) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("history", limit));

        /// <summary>
        /// Get the information available commits on this table as a Spark DataFrame. The
        /// information is in reverse chronological order.
        /// </summary>
        /// <returns>History DataFrame</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DataFrame History() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("history"));

        /// <summary>
        /// Generate a manifest for the given Delta Table.
        /// </summary>
        /// <param name="mode">Specifies the mode for the generation of the manifest.
        /// The valid modes are as follows (not case sensitive):
        /// - "symlink_format_manifest" : This will generate manifests in symlink format
        /// for Presto and Athena read support.
        /// See the online documentation for more information.</param>
        [DeltaLakeSince(DeltaLakeVersions.V0_5_0)]
        public void Generate(string mode) => Reference.Invoke("generate", mode);

        /// <summary>
        /// Delete data from the table that match the given <c>condition</c>.
        /// </summary>
        /// <param name="condition">Boolean SQL expression.</param>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public void Delete(string condition) => Reference.Invoke("delete", condition);

        /// <summary>
        /// Delete data from the table that match the given <c>condition</c>.
        /// </summary>
        /// <param name="condition">Boolean SQL expression.</param>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public void Delete(Column condition) => Reference.Invoke("delete", condition);

        /// <summary>
        /// Delete data from the table.
        /// </summary>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public void Delete() => Reference.Invoke("delete");

        /// <summary>
        /// Update rows in the table based on the rules defined by <c>set</c>.
        /// </summary>
        /// <example>
        /// Example to increment the column <c>data</c>.
        /// <code>
        /// deltaTable.Update(new Dictionary&lt;string, Column&gt;(){
        ///     {"data" , table.Col("data").Plus(1) }   
        /// })
        /// </code>
        /// </example>
        /// <param name="set">Pules to update a row as a Scala map between target column names
        /// and corresponding update expressions as Column objects.</param>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public void Update(Dictionary<string, Column> set) => Reference.Invoke("update", set);

        /// <summary>
        /// Update data from the table on the rows that match the given <c>condition</c> based on
        /// the rules defined by <c>set</c>.
        /// </summary>
        /// <example>
        /// Example to increment the column <c>data</c>.
        /// <code>
        /// deltaTable.Update(
        ///     table.Col("date").Gt("2018-01-01")
        ///     new Dictionary&lt;string, Column&gt;(){
        ///         {"data" , table.Col("data").Plus(1) }   
        ///     })
        /// </code>
        /// </example>
        /// <param name="condition">Boolean expression as Column object specifying which rows
        /// to update.</param>
        /// <param name="set">Rules to update a row as a Scala map between target column names and
        /// corresponding update expressions as Column objects.</param>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public void Update(Column condition, Dictionary<string, Column> set) =>
            Reference.Invoke("update", condition, set);

        /// <summary>
        /// Update rows in the table based on the rules defined by <c>set</c>.
        /// </summary>
        /// <example>
        /// Example to increment the column <c>data</c>.
        /// <code>
        /// deltaTable.UpdateExpr(
        ///     new Dictionary&lt;string, string&gt;(){
        ///         {"data" , "data + 1" }   
        ///     })
        /// </code>
        /// </example>
        /// <param name="set">Rules to update a row as a Scala map between target column names and
        /// corresponding update expressions as SQL formatted strings.</param>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public void UpdateExpr(Dictionary<string, string> set) =>
            Reference.Invoke("updateExpr", set);

        /// <summary>
        /// Update data from the table on the rows that match the given <c>condition</c>, which
        /// performs the rules defined by <c>set</c>.
        /// </summary>
        /// <example>
        /// Example to increment the column <c>data</c>.
        /// <code>
        /// deltaTable.UpdateExpr(
        ///     "date > '2018-01-01'",
        ///     new Dictionary&lt;string, string&gt;(){
        ///         {"data" , "data + 1" }   
        /// })
        /// </code>
        /// </example>
        /// <param name="condition">Boolean expression as SQL formatted string object specifying 
        /// which rows to update.</param>
        /// <param name="set">Rules to update a row as a map between target column names and
        /// corresponding update expressions as SQL formatted strings.</param>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public void UpdateExpr(string condition, Dictionary<string, string> set) =>
            Reference.Invoke("updateExpr", condition, set);

        /// <summary>
        /// Merge data from the <c>source</c> DataFrame based on the given merge <c>condition</c>.
        /// This returns a <c>DeltaMergeBuilder</c> object that can be used to specify the
        /// update, delete, or insert actions to be performed on rows based on whether the rows
        /// matched the condition or not.
        ///
        /// See the <see cref="DeltaMergeBuilder"/> for a full description of this operation and
        /// what combinations of update, delete and insert operations are allowed.
        /// </summary>
        /// <example>
        /// See the <c>DeltaMergeBuilder</c> for a full description of this operation and what
        /// combinations of update, delete and insert operations are allowed.
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
        ///        new Dictionary&lt;String, String&gt;() {
        ///          {"value", "source.value"}
        ///        })
        ///     .WhenNotMatched()
        ///     .InsertExpr(
        ///        new Dictionary&lt;String, String&gt;() {
        ///         {"key", "source.key"};
        ///         {"value", "source.value"};
        ///       })
        ///     .Execute();
        /// </code>
        /// </example>
        /// <param name="source">Source Dataframe to be merged.</param>
        /// <param name="condition">Boolean expression as SQL formatted string.</param>
        /// <returns>DeltaMergeBuilder</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeBuilder Merge(DataFrame source, string condition) =>
            new DeltaMergeBuilder(
                (JvmObjectReference)Reference.Invoke(
                "merge",
                source,
                condition));

        /// <summary>
        /// Merge data from the <c>source</c> DataFrame based on the given merge <c>condition</c>.
        /// This class returns a <c>DeltaMergeBuilder</c> object that can be used to specify the
        /// update, delete, or insert actions to be performed on rows based on whether the rows
        /// matched the condition or not.
        ///
        /// See the <see cref="DeltaMergeBuilder"/> for a full description of this operation and
        /// what combinations of update, delete and insert operations are allowed.
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
        ///        new Dictionary&lt;String, String&gt;() {
        ///          {"value", "source.value"}
        ///        })
        ///     .WhenNotMatched()
        ///     .InsertExpr(
        ///        new Dictionary&lt;String, String&gt;() {
        ///         {"key", "source.key"},
        ///         {"value", "source.value"}
        ///       })
        ///     .Execute()
        /// </code>
        /// </example>
        /// <param name="source">Source Dataframe to be merged.</param>
        /// <param name="condition">Coolean expression as a Column object</param>
        /// <returns>DeltaMergeBuilder</returns>
        [DeltaLakeSince(DeltaLakeVersions.V0_3_0)]
        public DeltaMergeBuilder Merge(DataFrame source, Column condition) =>
            new DeltaMergeBuilder(
                (JvmObjectReference)Reference.Invoke(
                "merge",
                source,
                condition));

        /// <summary>
        /// Updates the protocol version of the table to leverage new features. Upgrading the reader version
        /// will prevent all clients that have an older version of Delta Lake from accessing this table.
        /// Upgrading the writer version will prevent older versions of Delta Lake to write to this table.
        /// The reader or writer version cannot be downgraded.
        /// 
        /// See online documentation and Delta's protocol specification at
        /// <see href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md">PROTOCOL.md</see> for more
        /// details.
        /// </summary>
        /// <param name="readerVersion">Version of the Delta read protocol.</param>
        /// <param name="writerVersion">Version of the Delta write protocol.</param>
        [DeltaLakeSince(DeltaLakeVersions.V0_8_0)]
        public void UpgradeTableProtocol(int readerVersion, int writerVersion) =>
            Reference.Invoke("upgradeTableProtocol", readerVersion, writerVersion);
    }
}
