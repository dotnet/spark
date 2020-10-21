// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Interface used to write a <see cref="DataFrame"/> to external storage using the v2
    /// API.
    /// </summary>
    [Since(Versions.V3_0_0)]
    public sealed class DataFrameWriterV2 : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal DataFrameWriterV2(JvmObjectReference jvmObject) => _jvmObject = jvmObject;

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Specifies a provider for the underlying output data source. Spark's default catalog
        /// supports "parquet", "json", etc.
        /// </summary>
        /// <param name="provider">Provider name</param>
        /// <returns>This DataFrameWriterV2 object</returns>
        public DataFrameWriterV2 Using(string provider)
        {
            _jvmObject.Invoke("using", provider);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">string value of the option</param>
        /// <returns>This DataFrameWriterV2 object</returns>
        public DataFrameWriterV2 Option(string key, string value)
        {
            _jvmObject.Invoke("option", key, value);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">bool value of the option</param>
        /// <returns>This DataFrameWriterV2 object</returns>
        public DataFrameWriterV2 Option(string key, bool value)
        {
            _jvmObject.Invoke("option", key, value);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Long value of the option</param>
        /// <returns>This DataFrameWriterV2 object</returns>
        public DataFrameWriterV2 Option(string key, long value)
        {
            _jvmObject.Invoke("option", key, value);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        /// <param name="key">Name of the option</param>
        /// <param name="value">Double value of the option</param>
        /// <returns>This DataFrameWriterV2 object</returns>
        public DataFrameWriterV2 Option(string key, double value)
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
        /// <returns>This DataFrameWriterV2 object</returns>
        public DataFrameWriterV2 TableProperty(string property, string value)
        {
            _jvmObject.Invoke("tableProperty", property, value);
            return this;
        }

        /// <summary>
        /// Partition the output table created by <see cref="Create"/>,
        /// <see cref="CreateOrReplace"/>, or <see cref="Replace"/> using the given columns or
        /// transforms.
        /// </summary>
        /// <param name="column">Column name to partition on</param>
        /// <param name="columns">Columns to partition on</param>
        /// <returns>This DataFrameWriterV2 object</returns>
        public DataFrameWriterV2 PartitionedBy(Column column, params Column[] columns)
        {
            _jvmObject.Invoke("partitionedBy", column, columns);
            return this;
        }

        /// <summary>
        /// Create a new table from the contents of the data frame.
        /// </summary>
        public void Create() => _jvmObject.Invoke("create");

        /// <summary>
        /// Replace an existing table with the contents of the data frame.
        /// </summary>
        public void Replace() => _jvmObject.Invoke("replace");

        /// <summary>
        /// Create a new table or replace an existing table with the contents of the data frame.
        /// </summary>
        public void CreateOrReplace() => _jvmObject.Invoke("createOrReplace");

        /// <summary>
        /// Append the contents of the data frame to the output table.
        /// </summary>
        public void Append() => _jvmObject.Invoke("append");

        /// <summary>
        /// Overwrite rows matching the given filter condition with the contents of the data frame
        /// in the output table.
        /// </summary>
        /// <param name="condition">Condition filter to overwrite based on</param>
        public void Overwrite(Column condition) => _jvmObject.Invoke("overwrite", condition);

        /// <summary>
        /// Overwrite all partition for which the data frame contains at least one row with the
        /// contents of the data frame in the output table.
        /// </summary>
        public void OverwritePartitions() => _jvmObject.Invoke("overwritePartitions");
    }
}
