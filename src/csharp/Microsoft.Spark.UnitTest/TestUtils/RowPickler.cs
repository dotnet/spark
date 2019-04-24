// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Text;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Razorvine.Pickle;

namespace Microsoft.Spark.UnitTest.TestUtils
{
    /// <summary>
    /// Custom pickler for StructType objects.
    /// Refer to
    /// spark/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvaluatePython.scala
    /// </summary>
    internal class StructTypePickler : IObjectPickler
    {
        private readonly string _module = "pyspark.sql.types";

        public void Register()
        {
            Pickler.registerCustomPickler(GetType(), this);
            Pickler.registerCustomPickler(typeof(StructType), this);
        }

        public void pickle(object o, Stream stream, Pickler currentPickler)
        {
            if (!(o is StructType schema))
            {
                throw new InvalidOperationException("A StructType object is expected.");
            }

            SerDe.Write(stream, Opcodes.GLOBAL);
            SerDe.Write(stream, Encoding.UTF8.GetBytes(
                    $"{_module}\n_parse_datatype_json_string\n"));
            currentPickler.save(schema.Json);
            SerDe.Write(stream, Opcodes.TUPLE1);
            SerDe.Write(stream, Opcodes.REDUCE);
        }
    }

    /// <summary>
    /// Custom pickler for Row objects.
    /// Refer to
    /// spark/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvaluatePython.scala
    /// </summary>
    internal class RowPickler : IObjectPickler
    {
        private readonly string _module = "pyspark.sql.types";

        public void Register()
        {
            Pickler.registerCustomPickler(GetType(), this);
            Pickler.registerCustomPickler(typeof(Row), this);
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
                if (!(o is Row row))
                {
                    throw new InvalidOperationException("A Row object is expected.");
                }

                currentPickler.save(this);
                currentPickler.save(row.Schema);
                SerDe.Write(stream, Opcodes.TUPLE1);
                SerDe.Write(stream, Opcodes.REDUCE);

                SerDe.Write(stream, Opcodes.MARK);
                for (int i = 0; i < row.Size(); ++i)
                {
                    currentPickler.save(row.Get(i));
                }

                SerDe.Write(stream, Opcodes.TUPLE);
                SerDe.Write(stream, Opcodes.REDUCE);
            }
        }
    }
}
