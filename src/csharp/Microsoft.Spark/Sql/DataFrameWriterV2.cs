// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Interface used to write a [[org.apache.spark.sql.Dataset]] to external storage using the v2
    /// API.
    /// </summary>
    [Since(Versions.V3_0_0)]
    public sealed class DataFrameWriterV2 : IJvmObjectReferenceProvider, CreateTableWriter
    {
        private readonly JvmObjectReference _jvmObject;

        internal DataFrameWriterV2(JvmObjectReference jvmObject) => _jvmObject = jvmObject;

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Specifies a provider for the underlying output data source. Spark's default catalog
        /// supports "parquet", "json", etc.
        /// </summary>
        /// <param name="provider">Provider name</param>
        /// <returns>CreateTableWriter instance</returns>
        public CreateTableWriter Using(string provider)
        {
            return (CreateTableWriter)_jvmObject.Invoke("using", provider);
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Value of the option</param>
        /// <returns>This DataFrameWriterV2 object</returns>
        public DataFrameWriterV2 Option(string key, string value)
        {
            _jvmObject.Invoke("option", key, value);
            return this;
        }

        /// <summary>
        /// Adds output options for the underlying data source.
        /// </summary>
        /// <param name="options">Key/value options</param>
        /// <returns>This DataFrameWriterV2 object</returns>
        public DataFrameWriterV2 Options(Dictionary<string, string> options)
        {
            _jvmObject.Invoke("options", options);
            return this;
        }

        /// <summary>
        /// Add a table property.
        /// </summary>
        /// <param name="property">Name of property</param>
        /// <param name="value">Value of the property</param>
        /// <returns>CreateTableWriter instance</returns>
        public CreateTableWriter TableProperty(string property, string value)
        {
            return (CreateTableWriter)_jvmObject.Invoke("tableProperty", property, value);
        }

        /// <summary>
        /// Partition the output table created by `create`, `createOrReplace`, or `replace` using
        /// the given columns or transforms.
        /// </summary>
        /// <param name="column">Column name to partition on</param>
        /// <param name="columns">Columns to partition on</param>
        /// <returns>CreateTableWriter instance</returns>
        public CreateTableWriter PartitionedBy(Column column, params Column[] columns)
        {
            return (CreateTableWriter)_jvmObject.Invoke("partitionedBy", column, columns);
        }

        /// <summary>
        /// Create a new table from the contents of the data frame.
        /// </summary>
        public void Create()
        {
            _jvmObject.Invoke("create");
        }

        /// <summary>
        /// Replace an existing table with the contents of the data frame.
        /// </summary>
        public void Replace()
        {
            _jvmObject.Invoke("replace");
        }

        /// <summary>
        /// Create a new table or replace an existing table with the contents of the data frame.
        /// </summary>
        public void CreateOrReplace()
        {
            _jvmObject.Invoke("createOrReplace");
        }

        /// <summary>
        /// Append the contents of the data frame to the output table.
        /// </summary>
        public void Append()
        {
            _jvmObject.Invoke("append");
        }

        /// <summary>
        /// Overwrite rows matching the given filter condition with the contents of the data frame
        /// in the output table.
        /// </summary>
        /// <param name="condition">Condition filter to overwrite based on</param>
        public void Overwrite(Column condition)
        {
            _jvmObject.Invoke("overwrite", condition);
        }

        /// <summary>
        /// Overwrite all partition for which the data frame contains at least one row with the
        /// contents of the data frame in the output table.
        /// </summary>
        public void OverwritePartitions()
        {
            _jvmObject.Invoke("overwritePartitions");
        }
    }

    /// <summary>
    /// Interface to restrict calls to create and replace operations.
    /// </summary>
    [Since(Versions.V3_0_0)]
    public interface CreateTableWriter
    {
        /// <summary>
        /// Create a new table from the contents of the data frame.
        /// The new table's schema, partition layout, properties, and other configuration will be based
        /// on the configuration set on this writer.
        /// </summary>
        public void Create();

        /// <summary>
        /// Replace an existing table with the contents of the data frame.
        /// The existing table's schema, partition layout, properties, and other configuration will be
        /// replaced with the contents of the data frame and the configuration set on this writer.
        /// </summary>
        public void Replace();

        /// <summary>
        /// Create a new table or replace an existing table with the contents of the data frame.
        /// </summary>
        public void CreateOrReplace();

        /// <summary>
        /// Partition the output table created by `create`, `createOrReplace`, or `replace` using
        /// the given columns or transforms.
        /// </summary>
        /// <param name="column">Column name to partition on</param>
        /// <param name="columns">Columns to partition on</param>
        /// <returns>CreateTableWriter instance</returns>
        public CreateTableWriter PartitionedBy(Column column, params Column[] columns);

        /// <summary>
        /// Specifies a provider for the underlying output data source. Spark's default catalog
        /// supports "parquet", "json", etc.
        /// </summary>
        /// <param name="provider">Provider string value</param>
        /// <returns>CreateTableWriter instance</returns>
        public CreateTableWriter Using(string provider);

        /// <summary>
        /// Add a table property.
        /// </summary>
        /// <param name="property">Name of property</param>
        /// <param name="value">Value of the property</param>
        /// <returns>CreateTableWriter instance</returns>
        public CreateTableWriter TableProperty(string property, string value);
    }
}
