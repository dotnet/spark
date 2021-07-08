// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Sql.Streaming
{
    /// <summary>
    /// DataStreamReader provides functionality to load a streaming <see cref="DataFrame"/>
    /// from external storage systems (e.g. file systems, key-value stores, etc).
    /// </summary>
    public sealed class DataStreamReader : IJvmObjectReferenceProvider
    {
        internal DataStreamReader(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Specifies the input data source format.
        /// </summary>
        /// <param name="source">Name of the data source</param>
        /// <returns>This DataStreamReader object</returns>
        public DataStreamReader Format(string source)
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
        /// <returns>This DataStreamReader object</returns>
        public DataStreamReader Schema(StructType schema)
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
        /// <returns>This DataStreamReader object</returns>
        public DataStreamReader Schema(string schemaString)
        {
            Reference.Invoke("schema", schemaString);
            return this;
        }

        /// <summary>
        /// Adds an input option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataStreamReader object</returns>
        public DataStreamReader Option(string key, string value)
        {
            OptionInternal(key, value);
            return this;
        }

        /// <summary>
        /// Adds an input option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataStreamReader object</returns>
        public DataStreamReader Option(string key, bool value)
        {
            OptionInternal(key, value);
            return this;
        }

        /// <summary>
        /// Adds an input option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataStreamReader object</returns>
        public DataStreamReader Option(string key, long value)
        {
            OptionInternal(key, value);
            return this;
        }

        /// <summary>
        /// Adds an input option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataStreamReader object</returns>
        public DataStreamReader Option(string key, double value)
        {
            OptionInternal(key, value);
            return this;
        }

        /// <summary>
        /// Adds input options for the underlying data source.
        /// </summary>
        /// <param name="options">Key/value options</param>
        /// <returns>This DataStreamReader object</returns>
        public DataStreamReader Options(Dictionary<string, string> options)
        {
            Reference.Invoke("options", options);
            return this;
        }

        /// <summary>
        /// Loads input data stream in as a <see cref="DataFrame"/>, for data streams
        /// that don't require a path (e.g.external key-value stores).
        /// </summary>
        /// <returns>DataFrame object</returns>
        public DataFrame Load() => new DataFrame((JvmObjectReference)Reference.Invoke("load"));

        /// <summary>
        /// Loads input in as a <see cref="DataFrame"/>, for data streams that read
        /// from some path. 
        /// </summary>
        /// <param name="path">File path for streaming</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Load(string path) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("load", path));

        /// <summary>
        /// Loads a JSON file stream and returns the results as a <see cref="DataFrame"/>.
        /// </summary>
        /// <param name="path">File path for streaming</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Json(string path) => LoadSource("json", path);

        /// <summary>
        /// Loads a CSV file stream and returns the result as a <see cref="DataFrame"/>.
        /// </summary>
        /// <param name="path">File path for streaming</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Csv(string path) => LoadSource("csv", path);

        /// <summary>
        /// Loads a ORC file stream and returns the result as a <see cref="DataFrame"/>.
        /// </summary>
        /// <param name="path">File path for streaming</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Orc(string path) => LoadSource("orc", path);

        /// <summary>
        /// Loads a Parquet file stream and returns the result as a <see cref="DataFrame"/>.
        /// </summary>
        /// <param name="path">File path for streaming</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Parquet(string path) => LoadSource("parquet", path);

        /// <summary>
        /// Define a Streaming DataFrame on a Table. The DataSource corresponding to the table should
        /// support streaming mode.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <returns>DataFrame object</returns>
        [Since(Versions.V3_1_0)]
        public DataFrame Table(string tableName) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("table", tableName));

        /// <summary>
        /// Loads text files and returns a <see cref="DataFrame"/> whose schema starts
        /// with a string column named "value", and followed by partitioned columns
        /// if there are any.
        /// </summary>
        /// <param name="path">File path for streaming</param>
        /// <returns>DataFrame object</returns>
        public DataFrame Text(string path) => LoadSource("text", path);

        /// <summary>
        /// Helper function to add given key/value pair as a new option.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameReader object</returns>
        private DataStreamReader OptionInternal(string key, object value)
        {
            Reference.Invoke("option", key, value);
            return this;
        }

        /// <summary>
        /// Helper function to load the source for a given path.
        /// </summary>
        /// <param name="source">Name of the source</param>
        /// <param name="path">File path for streaming</param>
        /// <returns>DataFrame object</returns>
        private DataFrame LoadSource(string source, string path) =>
            new DataFrame((JvmObjectReference)Reference.Invoke(source, path));
    }
}
