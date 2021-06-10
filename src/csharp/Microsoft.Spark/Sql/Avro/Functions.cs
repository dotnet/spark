// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Avro
{
    /// <summary>
    /// Functions for serialization and deserialization of data in Avro format.
    /// </summary>
    public static class Functions
    {
        private static IJvmBridge Jvm { get; } = SparkEnvironment.JvmBridge;
        private static readonly string s_30functionsClassName = "org.apache.spark.sql.avro.functions";
        private static readonly string s_24functionsClassName = "org.apache.spark.sql.avro.package";

        /// <summary>
        /// Converts a binary column of avro format into its corresponding catalyst value. The specified
        /// schema must match the read data, otherwise the behavior is undefined: it may fail or return
        /// arbitrary result.
        /// </summary>
        /// <param name="data">The binary column.</param>
        /// <param name="jsonFormatSchema">The avro schema in JSON string format.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column FromAvro(Column data, string jsonFormatSchema)
        {
            Version sparkVersion = SparkEnvironment.SparkVersion;
            string className = sparkVersion.Major switch
            {
                2 => s_24functionsClassName,
                3 => s_30functionsClassName,
                _ => throw new NotSupportedException($"Spark {sparkVersion} not supported.")
            };
            return new Column(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    className,
                    "from_avro",
                    data,
                    jsonFormatSchema));
        }

        /// <summary>
        /// Converts a binary column of avro format into its corresponding catalyst value. The specified
        /// schema must match the read data, otherwise the behavior is undefined: it may fail or return
        /// arbitrary result. To deserialize the data with a compatible and evolved schema, the expected Avro
        /// schema can be set via the option avroSchema.
        /// </summary>
        /// <param name="data">The binary column.</param>
        /// <param name="jsonFormatSchema">The avro schema in JSON string format.</param>
        /// <param name="options">Options to control how the Avro record is parsed.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column FromAvro(
            Column data,
            string jsonFormatSchema,
            Dictionary<string, string> options)
        {
            return new Column(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_30functionsClassName,
                    "from_avro",
                    data,
                    jsonFormatSchema,
                    options));
        }

        /// <summary>
        /// Converts a column into binary of avro format.
        /// </summary>
        /// <param name="data">The data column.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V2_4_0)]
        public static Column ToAvro(Column data)
        {
            Version sparkVersion = SparkEnvironment.SparkVersion;
            string className = sparkVersion.Major switch
            {
                2 => s_24functionsClassName,
                3 => s_30functionsClassName,
                _ => throw new NotSupportedException($"Spark {sparkVersion} not supported.")
            };
            return new Column(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    className,
                    "to_avro",
                    data));
        }

        /// <summary>
        /// Converts a column into binary of avro format.
        /// </summary>
        /// <param name="data">The data column.</param>
        /// <param name="jsonFormatSchema">User-specified output avro schema in JSON string format.</param>
        /// <returns>Column object</returns>
        [Since(Versions.V3_0_0)]
        public static Column ToAvro(Column data, string jsonFormatSchema)
        {
            return new Column(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_30functionsClassName,
                    "to_avro",
                    data,
                    jsonFormatSchema));
        }
    }
}