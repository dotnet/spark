// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Spark.Sql.Types;
using Razorvine.Pickle;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// RowConstructor is a custom unpickler for GenericRowWithSchema in Spark.
    /// Refer to spark/sql/core/src/main/scala/org/apache/spark/sql/execution/python/
    /// EvaluatePython.scala how GenericRowWithSchema is being pickled.
    /// </summary>
    internal sealed class RowConstructor : IObjectConstructor
    {
        /// <summary>
        /// Cache the schemas of the rows being received. Multiple schemas may be
        /// sent per batch if there are nested rows contained in the row. Note that
        /// this is thread local variable because one RowConstructor object is
        /// registered to the Unpickler and there could be multiple threads unpickling
        /// the data using the same registered object.
        /// </summary>
        [ThreadStatic]
        private static IDictionary<string, StructType> s_schemaCache;

        /// <summary>
        /// Used by Unpickler to pass unpickled schema for handling. The Unpickler
        /// will reuse the <see cref="RowConstructor"/> object when
        /// it needs to start constructing a <see cref="Row"/>. The schema is passed
        /// to <see cref="construct(object[])"/> and the returned
        /// <see cref="IObjectConstructor"/> is used to build the rest of the <see cref="Row"/>.
        /// </summary>
        /// <param name="args">Unpickled schema</param>
        /// <returns>
        /// New <see cref="RowWithSchemaConstructor"/>object capturing the schema.
        /// </returns>
        public object construct(object[] args)
        {
            if (s_schemaCache is null)
            {
                s_schemaCache = new Dictionary<string, StructType>();
            }

            Debug.Assert((args != null) && (args.Length == 1) && (args[0] is string));
            return new RowWithSchemaConstructor(GetSchema(s_schemaCache, (string)args[0]));
        }

        /// <summary>
        /// Clears the schema cache. Spark sends rows in batches and for each
        /// row there is an accompanying set of schemas and row entries. If the
        /// schema was not cached, then it would need to be parsed and converted
        /// to a StructType for every row in the batch. A new batch may contain
        /// rows from a different table, so calling <see cref="Reset"/> after each
        /// batch would aid in preventing the cache from growing too large.
        /// Caching the schemas for each batch, ensures that each schema is
        /// only parsed and converted to a StructType once per batch.
        /// </summary>
        internal void Reset()
        {
            s_schemaCache?.Clear();
        }

        private static StructType GetSchema(IDictionary<string, StructType> schemaCache, string schemaString)
        {
            if (!schemaCache.TryGetValue(schemaString, out StructType schema))
            {
                schema = (StructType)DataType.ParseDataType(schemaString);
                schemaCache.Add(schemaString, schema);
            }

            return schema;
        }
    }

    /// <summary>
    /// Created from <see cref="RowConstructor"/> and subsequently used
    /// by the Unpickler to construct a <see cref="Row"/>.
    /// </summary>
    internal sealed class RowWithSchemaConstructor : IObjectConstructor
    {
        private readonly StructType _schema;

        internal RowWithSchemaConstructor(StructType schema)
        {
            _schema = schema;
        }

        /// <summary>
        /// Used by Unpickler to pass unpickled row values for handling.
        /// </summary>
        /// <param name="args">Unpickled row values.</param>
        /// <returns>Row object.</returns>
        public object construct(object[] args) => new Row(args, _schema);
    }
}
