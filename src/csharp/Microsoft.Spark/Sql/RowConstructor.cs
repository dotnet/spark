// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
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
        /// Cache the schema of the rows being received. Note that this is thread local variable
        /// because one RowConstructor object is registered to the Unpickler and there
        /// could be multiple threads unpickling the data using the same object registered.
        /// </summary>
        [ThreadStatic]
        private static ConcurrentDictionary<string, StructType> s_schemaCache;

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
            if (s_schemaCache is null)
            {
                s_schemaCache = new ConcurrentDictionary<string, StructType>();
            }

            if ((args.Length == 1) && (args[0] is RowConstructor))
            {
                args[0] = ((RowConstructor)args[0]).GetRow();
                return args;
            }

            return new RowConstructor(this, args);
        }

        /// <summary>
        /// Construct a Row object from unpickled data.
        /// </summary>
        /// <returns>A row object with unpickled data</returns>
        public Row GetRow()
        {
            Debug.Assert(_parent != null);

            for(int i = 0; i < _args.Length; ++i)
            {
                if (_args[i] is RowConstructor)
                {
                    _args[i] = ((RowConstructor)_args[i]).GetRow();
                }
            }

            return new Row(_args, _parent.GetSchema());
        }

        internal void Reset()
        {
            s_schemaCache.Clear();
        }

        /// <summary>
        /// Get or cache the schema string contained in args. Calling this
        /// is only valid if the child args contain the row values.
        /// </summary>
        /// <returns></returns>
        private StructType GetSchema()
        {
            Debug.Assert((_args != null) && (_args.Length == 1) && (_args[0] is string));
            string schemaString = _args[0] as string;
            return s_schemaCache
                .GetOrAdd(schemaString, s => (StructType)DataType.ParseDataType(s));
        }
    }
}
