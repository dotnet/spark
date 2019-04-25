// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// DataFrameReader provides functionality to load a <see cref="DataFrame"/>
    /// from external storage systems (e.g. file systems, key-value stores, etc).
    /// </summary>
    public sealed class DataFrameReader : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal DataFrameReader(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Specifies the input data source format.
        /// </summary>
        /// <param name="source">Name of the data source</param>
        /// <returns>This DataFrameReader object</returns>
        public DataFrameReader Format(string source)
        {
            _jvmObject.Invoke("format", source);
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
            _jvmObject.Invoke("schema", schemaString);
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
            _jvmObject.Invoke("options", options);
            return this;
        }

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
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("load", paths));

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
            _jvmObject.Invoke("option", key, value);
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

            return new DataFrame((JvmObjectReference)_jvmObject.Invoke(source, paths));
        }
    }
}
