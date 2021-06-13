// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// DataFrameReader provides functionality to load a <see cref="DataFrame"/>
    /// from external storage systems (e.g. file systems, key-value stores, etc).
    /// </summary>
    public sealed class DataFrameReader : IJvmObjectReferenceProvider
    {
        internal DataFrameReader(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Specifies the input data source format.
        /// </summary>
        /// <param name="source">Name of the data source</param>
        /// <returns>This DataFrameReader object</returns>
        public DataFrameReader Format(string source)
        {
            Reference.Invoke("format", source);
            return this;
        }
        
        /// <summary>
        /// Specifies the schema by using <see cref="StructType"/>.
        /// </summary>
        /// <remarks>
        /// Some data sources (e.g. JSON) can infer the input schema automatically
        /// from data. By specifying the schema here, the underlying data source can
        /// skip the schema inference step, and thus speed up data loading.
        /// </remarks>
        /// <param name="schema">The input schema</param>
        /// <returns>This DataFrameReader object</returns>
        public DataFrameReader Schema(StructType schema)
        {
            Reference.Invoke("schema", DataType.FromJson(Reference.Jvm, schema.Json));
            return this;
        }
        
        /// <summary>
        /// Specifies the schema by using the given DDL-formatted string.
        /// </summary>
        /// <remarks>
        /// Some data sources (e.g. JSON) can infer the input schema automatically
        /// from data. By specifying the schema here, the underlying data source can
        /// skip the schema inference step, and thus speed up data loading.
        /// </remarks>
        /// <param name="schemaString">DDL-formatted string</param>
        /// <returns>This DataFrameReader object</returns>
        public DataFrameReader Schema(string schemaString)
        {
            Reference.Invoke("schema", schemaString);
            return this;
        }

        /// <summary>
        /// Adds an input option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameReader object</returns>
        public DataFrameReader Option(string key, string value)
        {
            return OptionInternal(key, value);
        }

        /// <summary>
        /// Adds an input option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameReader object</returns>
        public DataFrameReader Option(string key, bool value)
        {
            return OptionInternal(key, value);
        }

        /// <summary>
        /// Adds an input option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameReader object</returns>
        public DataFrameReader Option(string key, long value)
        {
            return OptionInternal(key, value);
        }

        /// <summary>
        /// Adds an input option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameReader object</returns>
        public DataFrameReader Option(string key, double value)
        {
            return OptionInternal(key, value);
        }

        /// <summary>
        /// Adds input options for the underlying data source.
        /// </summary>
        /// <param name="options">Key/value options</param>
        /// <returns>This DataFrameReader object</returns>
        public DataFrameReader Options(Dictionary<string, string> options)
        {
            Reference.Invoke("options", options);
            return this;
        }

        /// <summary>
        /// Loads input in as a DataFrame, for data sources that don't require a path
        /// (e.g. external key-value stores).
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Load() => new DataFrame((JvmObjectReference)Reference.Invoke("load"));

        /// <summary>
        /// Loads input in as a DataFrame, for data sources that require a path 
        /// (e.g. data backed by a local or distributed file system).
        /// </summary>
        /// <param name="path">Input path</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Load(string path) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("load", path));

        /// <summary>
        /// Loads input in as a DataFrame from the given paths.
        /// </summary>
        /// <remarks>
        /// Paths can be empty if data sources don't require a path (e.g. external
        /// key-value stores).
        /// </remarks>
        /// <param name="paths">Input paths</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Load(params string[] paths) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("load", (object)paths));

        /// <summary>
        /// Construct a DataFrame representing the database table accessible via JDBC URL
        /// url named table and connection properties.
        /// </summary>
        /// <param name="url">JDBC database url of the form "jdbc:subprotocol:subname"</param>
        /// <param name="table">Name of the table in the external database</param>
        /// <param name="properties">JDBC database connection arguments</param>
        /// <returns>DataFrame representing the database table accessible via JDBC</returns>
        public DataFrame Jdbc(string url, string table, Dictionary<string, string> properties) =>
            new DataFrame((JvmObjectReference)Reference.Invoke(
                "jdbc",
                url,
                table,
                new Properties(Reference.Jvm, properties)));

        /// <summary>
        /// Construct a DataFrame representing the database table accessible via JDBC URL
        /// url named table. Partitions of the table will be retrieved in parallel based
        /// on the parameters passed to this function.
        /// </summary>
        /// <param name="url">JDBC database url of the form "jdbc:subprotocol:subname"</param>
        /// <param name="table">Name of the table in the external database</param>
        /// <param name="columnName">The name of a column of integral type that will be used
        /// for partitioning</param>
        /// <param name="lowerBound">The minimum value of columnName used to decide partition
        /// stride.</param>
        /// <param name="upperBound">The maximum value of columnName used to decide partition
        /// stride</param>
        /// <param name="numPartitions">
        /// The number of partitions. This, along with lowerBound (inclusive),
        /// upperBound(exclusive), form partition strides for generated WHERE
        /// clause expressions used to split the column columnName evenly.When
        /// the input is less than 1, the number is set to 1.
        /// </param>
        /// <param name="properties">JDBC database connection arguments</param>
        /// <returns>DataFrame representing the database table accessible via JDBC</returns>
        public DataFrame Jdbc(
            string url,
            string table,
            string columnName,
            long lowerBound,
            long upperBound,
            int numPartitions,
            Dictionary<string, string> properties) =>
            new DataFrame((JvmObjectReference)Reference.Invoke(
                "jdbc",
                url,
                table,
                columnName,
                lowerBound,
                upperBound,
                numPartitions,
                new Properties(Reference.Jvm, properties)));

        /// <summary>
        /// Construct a DataFrame representing the database table accessible via JDBC URL
        /// url named table and connection properties. The predicates parameter gives a list
        /// expressions suitable for inclusion in WHERE clauses; each one defines one partition
        /// of the DataFrame.
        /// </summary>
        /// <param name="url">JDBC database url of the form "jdbc:subprotocol:subname"</param>
        /// <param name="table">Name of the table in the external database</param>
        /// <param name="predicates">Condition in the WHERE clause for each partition</param>
        /// <param name="properties">JDBC database connection arguments</param>
        /// <returns>DataFrame representing the database table accessible via JDBC</returns>
        public DataFrame Jdbc(
            string url,
            string table,
            IEnumerable<string> predicates,
            Dictionary<string, string> properties) =>
            new DataFrame((JvmObjectReference)Reference.Invoke(
                "jdbc",
                url,
                table,
                predicates,
                new Properties(Reference.Jvm, properties)));

        /// <summary>
        /// Loads a JSON file (one object per line) and returns the result as a DataFrame.
        /// </summary>
        /// <param name="paths">Input paths</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Json(params string[] paths) => LoadSource("json", paths);

        /// <summary>
        /// Loads CSV files and returns the result as a DataFrame.
        /// </summary>
        /// <param name="paths">Input paths</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Csv(params string[] paths) => LoadSource("csv", paths);

        /// <summary>
        /// Loads a Parquet file, returning the result as a DataFrame.
        /// </summary>
        /// <param name="paths">Input paths</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Parquet(params string[] paths) => LoadSource("parquet", paths);

        /// <summary>
        /// Loads an ORC file and returns the result as a DataFrame.
        /// </summary>
        /// <param name="paths">Input paths</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Orc(params string[] paths) => LoadSource("orc", paths);
        
        /// <summary>
        /// Returns the specified table as a DataFrame.
        /// </summary>
        /// <param name="tableName">Name of the table to read</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Table(string tableName) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("table", tableName));

        /// <summary>
        /// Loads text files and returns a DataFrame whose schema starts with a string column
        /// named "value", and followed by partitioned columns if there are any.
        /// </summary>
        /// <param name="paths">Input paths</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Text(params string[] paths) => LoadSource("text", paths);

        /// <summary>
        /// Helper function to add given key/value pair as a new option.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameReader object</returns>
        private DataFrameReader OptionInternal(string key, object value)
        {
            Reference.Invoke("option", key, value);
            return this;
        }

        /// <summary>
        /// Helper function to create a DataFrame with the given source and paths.
        /// </summary>
        /// <param name="source">Name of the data source</param>
        /// <param name="paths">Input paths</param>
        /// <returns>A DataFrame object</returns>
        private DataFrame LoadSource(string source, params string[] paths)
        {
            if (paths.Length == 0)
            {
                throw new ArgumentException($"paths cannot be empty for source: {source}");
            }

            return new DataFrame((JvmObjectReference)Reference.Invoke(source, (object)paths));
        }
    }
}
