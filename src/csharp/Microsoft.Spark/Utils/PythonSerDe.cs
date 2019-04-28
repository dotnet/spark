// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.IO;
using Microsoft.Spark.Sql;
using Razorvine.Pickle;
using Razorvine.Pickle.Objects;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// Used for SerDe of Python objects.
    /// </summary>
    internal class PythonSerDe
    {
        static PythonSerDe()
        {
            // Custom picklers used in PySpark implementation.
            // Refer to spark/python/pyspark/sql/types.py.
            Unpickler.registerConstructor(
                "pyspark.sql.types", "_parse_datatype_json_string", new StringConstructor());
            Unpickler.registerConstructor(
                "pyspark.sql.types", "_create_row_inbound_converter", new RowConstructor());
            BufferUnpickler.registerConstructor(
                "pyspark.sql.types", "_parse_datatype_json_string", new StringConstructor());
            BufferUnpickler.registerConstructor(
                "pyspark.sql.types", "_create_row_inbound_converter", new RowConstructor());
        }

        /// <summary>
        /// Unpickles objects from byte[].
        /// </summary>
        /// <param name="s">Pickled byte stream</param>
        /// <returns>Unpicked objects</returns>
        internal static object[] GetUnpickledObjects(Stream s)
        {
            // Not making any assumptions about the implementation and hence not a class member.
            var unpickler = new Unpickler();
            var unpickledItems = unpickler.load(s);
            Debug.Assert(unpickledItems != null);
            return (unpickledItems as object[]);
        }

        internal static object[] GetUnpickledObjects(ReadOnlySpan<byte> memory)
        {
            var unpickler = new BufferUnpickler();
            var unpickledItems = unpickler.load(memory);
            Debug.Assert(unpickledItems != null);
            return (unpickledItems as object[]);
        }
    }
}
