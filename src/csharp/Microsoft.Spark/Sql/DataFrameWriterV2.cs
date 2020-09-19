// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Interface used to write a [[org.apache.spark.sql.Dataset]] to external storage using the v2
    /// API.
    /// </summary>
    [Since(Versions.V3_0_0)]
    public sealed class DataFrameWriterV2 : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal DataFrameWriterV2(JvmObjectReference jvmObject) => _jvmObject = jvmObject;

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

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

        
    }
}
