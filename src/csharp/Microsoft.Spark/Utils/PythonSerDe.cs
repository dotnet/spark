// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
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
        // One RowConstructor object is registered to the Unpickler and
        // there could be multiple threads unpickling row data using
        // this object. However there is no issue as the field(s) that are
        // reused by this object are instantiated on a per-thread basis and
        // therefore not shared between threads.
        private static readonly RowConstructor s_rowConstructor;

        static PythonSerDe()
        {
            // Custom picklers used in PySpark implementation.
            // Refer to spark/python/pyspark/sql/types.py.
            Unpickler.registerConstructor(
                "pyspark.sql.types", "_parse_datatype_json_string", new StringConstructor());

            s_rowConstructor = new RowConstructor();
            Unpickler.registerConstructor(
                "pyspark.sql.types", "_create_row_inbound_converter", s_rowConstructor);
        }

        /// <summary>
        /// Unpickles objects from Stream.
        /// </summary>
        /// <param name="stream">Pickled byte stream</param>
        /// <param name="messageLength">Size (in bytes) of the pickled input</param>
        /// <returns>Unpicked objects</returns>
        internal static object[] GetUnpickledObjects(Stream stream, int messageLength)
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(messageLength);

            try
            {
                if (!SerDe.TryReadBytes(stream, buffer, messageLength))
                {
                    throw new ArgumentException("The stream is closed.");
                }

                var unpickler = new Unpickler();
                object unpickledItems = unpickler.loads(
                    new ReadOnlyMemory<byte>(buffer, 0, messageLength), 
                    stackCapacity: 102); // Spark sends batches of 100 rows, and +2 is for markers.
                s_rowConstructor.Reset();
                Debug.Assert(unpickledItems != null);
                return (unpickledItems as object[]);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }
}
