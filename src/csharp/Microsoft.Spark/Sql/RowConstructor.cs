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
    /// EvaluatePython.scala how GenericRowWithSchema is being pickeld.
    /// </summary>
    internal sealed class RowConstructor : IObjectConstructor
    {
        /// <summary>
        /// Cache the schemas of the rows being received. Multiple schemas may be
        /// sent per batch if there are nested rows contained in the row. Note that
        /// this is thread local variable because one RowConstructor object is
        /// registered to the Unpickler and there could be multiple threads unpickling
        /// the data using the same object registered.
        /// </summary>
        [ThreadStatic]
        private static IDictionary<string, StructType> s_schemaCache;

        /// <summary>
        /// The RowConstructor that created this instance.
        /// </summary>
        private readonly RowConstructor _parent;

        /// <summary>
        /// Stores the args passed from construct().
        /// </summary>
        private readonly object[] _args;

        public RowConstructor() : this(null, null)
        {
        }

        public RowConstructor(RowConstructor parent, object[] args)
        {
            _parent = parent;
            _args = args;
        }

        /// <summary>
        /// Used by Unpickler to pass unpickled data for handling.
        /// </summary>
        /// <param name="args">Unpickled data</param>
        /// <returns>New RowConstructor object capturing args data</returns>
        public object construct(object[] args)
        {
            // Every first call to construct() contains the schema data. When
            // a new RowConstructor object is returned from this function,
            // construct() is called on the returned object with the actual
            // row data. The original RowConstructor object may be reused by the
            // Unpickler and each subsequent construct() call can contain the
            // schema data or a RowConstructor object that contains row data.
            if (s_schemaCache is null)
            {
                s_schemaCache = new Dictionary<string, StructType>();
            }

            // When a row is ready to be materialized, then construct() is called
            // on the RowConstructor which represents the row.
            if ((args.Length == 1) &&
                (args[0] is RowConstructor rowConstructor) &&
                (rowConstructor._parent == null))
            {
                // Construct the Row and return args containing the Row.
                args[0] = rowConstructor.GetRow();
                return args;
            }

            // Return a new RowConstructor where the args either represent the
            // schema or the row data. The parent becomes important when calling
            // GetRow() on the RowConstructor containing the row data.
            //
            // - When args is the schema, return a new RowConstructor where the
            // parent is set to the calling RowConstructor object.
            //
            // - In the case where args is the row data, construct() is called on a
            // RowConstructor object that contains the schema for the row data. A
            // new RowConstructor is returned where the parent is set to the schema
            // containing RowConstructor.
            return new RowConstructor(this, args);
        }

        /// <summary>
        /// Construct a Row object from unpickled data. This is only to be called
        /// on a RowConstructor that contains the row data.
        /// </summary>
        /// <returns>A row object with unpickled data</returns>
        public Row GetRow()
        {
            Debug.Assert(_parent != null);

            // It is possible that an entry of a Row (row1) may itself be a Row (row2).
            // If the entry is a RowConstructor then it will be a RowConstructor
            // which contains the data for row2. Therefore we will call GetRow()
            // on the RowConstructor to materialize row2 and replace the RowConstructor
            // entry in row1.
            for (int i = 0; i < _args.Length; ++i)
            {
                if (_args[i] is RowConstructor rowConstructor)
                {
                    _args[i] = rowConstructor.GetRow();
                }
            }

            return new Row(_args, _parent.GetSchema());
        }

        /// <summary>
        /// Clears the schema cache. Spark sends rows in batches and for each
        /// row there is an accompanying set of schemas and row entries. If the
        /// schema was not cached, then it would need to be parsed and converted
        /// to a StructType for every row in the batch. A new batch may contain
        /// rows from a different table, so calling <c>Reset</c> after each
        /// batch would aid in preventing the cache from growing too large.
        /// Caching the schemas for each batch, ensures that each schema is
        /// only parsed and converted to a StructType once per batch.
        /// </summary>
        internal void Reset()
        {
            s_schemaCache?.Clear();
        }

        /// <summary>
        /// Get or cache the schema string contained in args. Calling this
        /// is only valid if the child args contain the row values.
        /// </summary>
        /// <returns></returns>
        private StructType GetSchema()
        {
            Debug.Assert(s_schemaCache != null);
            Debug.Assert((_args != null) && (_args.Length == 1) && (_args[0] is string));
            var schemaString = (string)_args[0];
            if (!s_schemaCache.TryGetValue(schemaString, out StructType schema))
            {
                schema = (StructType)DataType.ParseDataType(schemaString);
                s_schemaCache.Add(schemaString, schema);
            }

            return schema;
        }
    }
}
