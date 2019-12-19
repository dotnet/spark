// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Text;
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

        /// <summary>
        /// Custom pickler for GenericRow objects.
        /// Refer to
        /// spark/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvaluatePython.scala
        /// </summary>
        internal class GenericRowPickler : IObjectPickler
        {
            private readonly string _module = "pyspark.sql.types";

            public void Register()
            {
                Pickler.registerCustomPickler(GetType(), this);
                Pickler.registerCustomPickler(typeof(GenericRow), this);
            }

            public void pickle(object o, Stream stream, Pickler currentPickler)
            {
                if (o.Equals(this))
                {
                    SerDe.Write(stream, Opcodes.GLOBAL);
                    SerDe.Write(stream, Encoding.UTF8.GetBytes(
                        $"{_module}\n_create_row_inbound_converter\n"));
                }
                else
                {
                    if (!(o is GenericRow genericRow))
                    {
                        throw new InvalidOperationException("A GenericRow object is expected.");
                    }

                    currentPickler.save(this);
                    SerDe.Write(stream, Opcodes.TUPLE1);
                    SerDe.Write(stream, Opcodes.REDUCE);

                    SerDe.Write(stream, Opcodes.MARK);
                    for (int i = 0; i < genericRow.Size(); ++i)
                    {
                        currentPickler.save(genericRow.Get(i));
                    }

                    SerDe.Write(stream, Opcodes.TUPLE);
                    SerDe.Write(stream, Opcodes.REDUCE);
                }
            }
        }

        internal static Pickler CreatePickler()
        {
            new GenericRowPickler().Register();
            return new Pickler();
        }
    }
}
