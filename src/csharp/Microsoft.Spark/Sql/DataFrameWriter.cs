// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Interface used to write a DataFrame to external storage systems (e.g. file systems,
    /// key-value stores, etc).
    /// </summary>
    public sealed class DataFrameWriter : IJvmObjectReferenceProvider
    {
        internal DataFrameWriter(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Specifies the behavior when data or table already exists.
        /// </summary>
        /// <remarks>
        /// Options include:
        ///   - SaveMode.Overwrite: overwrite the existing data.
        ///   - SaveMode.Append: append the data.
        ///   - SaveMode.Ignore: ignore the operation (i.e. no-op).
        ///   - SaveMode.ErrorIfExists: default option, throw an exception at runtime.
        /// </remarks>
        /// <param name="saveMode">Save mode enum</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter Mode(SaveMode saveMode) => Mode(saveMode.ToString());

        /// <summary>
        /// Specifies the behavior when data or table already exists.
        /// </summary>
        /// <remarks>
        /// Options include:
        ///   - "overwrite": overwrite the existing data.
        ///   - "append": append the data.
        ///   - "ignore": ignore the operation (i.e.no-op).
        ///   - "error" or "errorifexists": default option, throw an exception at runtime.
        /// </remarks>
        /// <param name="saveMode">Save mode string</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter Mode(string saveMode)
        {
            Reference.Invoke("mode", saveMode);
            return this;
        }

        /// <summary>
        /// Specifies the underlying output data source. Built-in options include
        /// "parquet", "json", etc.
        /// </summary>
        /// <param name="source">Data source name</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter Format(string source)
        {
            Reference.Invoke("format", source);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter Option(string key, string value)
        {
            return OptionInternal(key, value);
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter Option(string key, bool value)
        {
            return OptionInternal(key, value);
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter Option(string key, long value)
        {
            return OptionInternal(key, value);
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter Option(string key, double value)
        {
            return OptionInternal(key, value);
        }

        /// <summary>
        /// Adds output options for the underlying data source.
        /// </summary>
        /// <param name="options">Key/value options</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter Options(Dictionary<string, string> options)
        {
            Reference.Invoke("options", options);
            return this;
        }

        /// <summary>
        /// Partitions the output by the given columns on the file system. If specified,
        /// the output is laid out on the file system similar to Hive's partitioning scheme.
        /// </summary>
        /// <param name="colNames">Column names to partition by</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter PartitionBy(params string[] colNames)
        {
            Reference.Invoke("partitionBy", (object)colNames);
            return this;
        }

        /// <summary>
        /// Buckets the output by the given columns. If specified, the output is laid out
        /// on the file system similar to Hive's bucketing scheme.
        /// </summary>
        /// <param name="numBuckets">Number of buckets to save</param>
        /// <param name="colName">A column name</param>
        /// <param name="colNames">Additional column names</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter BucketBy(
            int numBuckets,
            string colName,
            params string[] colNames)
        {
            Reference.Invoke("bucketBy", numBuckets, colName, colNames);
            return this;
        }

        /// <summary>
        /// Sorts the output in each bucket by the given columns.
        /// </summary>
        /// <param name="colName">A name of a column</param>
        /// <param name="colNames">Additional column names</param>
        /// <returns>This DataFrameWriter object</returns>
        public DataFrameWriter SortBy(string colName, params string[] colNames)
        {
            Reference.Invoke("sortBy", colName, colNames);
            return this;
        }

        /// <summary>
        /// Saves the content of the DataFrame at the specified path.
        /// </summary>
        /// <param name="path">Path to save the content</param>
        public void Save(string path) => Reference.Invoke("save", path);

        /// <summary>
        /// Saves the content of the DataFrame as the specified table.
        /// </summary>
        public void Save() => Reference.Invoke("save");

        /// <summary>
        /// Inserts the content of the DataFrame to the specified table. It requires that
        /// the schema of the DataFrame is the same as the schema of the table.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        public void InsertInto(string tableName) => Reference.Invoke("insertInto", tableName);

        /// <summary>
        /// Saves the content of the DataFrame as the specified table.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        public void SaveAsTable(string tableName) => Reference.Invoke("saveAsTable", tableName);

        /// <summary>
        /// Saves the content of the DataFrame to a external database table via JDBC
        /// </summary>
        /// <param name="url">JDBC database URL of the form "jdbc:subprotocol:subname"</param>
        /// <param name="table">Name of the table in the external database</param>
        /// <param name="properties">JDBC database connection arguments</param>
        public void Jdbc(string url, string table, Dictionary<string, string> properties)
        {
            Reference.Invoke("jdbc", url, table, new Properties(Reference.Jvm, properties));
        }

        /// <summary>
        /// Saves the content of the DataFrame in JSON format at the specified path.
        /// </summary>
        /// <param name="path">Path to save the content</param>
        public void Json(string path) => Reference.Invoke("json", path);

        /// <summary>
        /// Saves the content of the DataFrame in Parquet format at the specified path.
        /// </summary>
        /// <param name="path">Path to save the content</param>
        public void Parquet(string path) => Reference.Invoke("parquet", path);

        /// <summary>
        /// Saves the content of the DataFrame in ORC format at the specified path.
        /// </summary>
        /// <param name="path">Path to save the content</param>
        public void Orc(string path) => Reference.Invoke("orc", path);

        /// <summary>
        /// Saves the content of the DataFrame in a text file at the specified path.
        /// </summary>
        /// <param name="path">Path to save the content</param>
        public void Text(string path) => Reference.Invoke("text", path);

        /// <summary>
        /// Saves the content of the DataFrame in CSV format at the specified path.
        /// </summary>
        /// <param name="path">Path to save the content</param>
        public void Csv(string path) => Reference.Invoke("csv", path);

        /// <summary>
        /// Helper function to add given key/value pair as a new option.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameWriter object</returns>
        private DataFrameWriter OptionInternal(string key, object value)
        {
            Reference.Invoke("option", key, value);
            return this;
        }
    }
}
