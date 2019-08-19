// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql.Types;
using Razorvine.Pickle;
using System.Diagnostics;
using Apache.Arrow;
using System.Collections.Concurrent;

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
        /// Schema of the rows being received. Note that this is thread local variable
        /// because one RowConstructor object is registered to the Unpickler and there
        /// could be multiple threads unpickling the data using the same object registered.
        /// </summary>
        [ThreadStatic]
        private static ConcurrentDictionary<string, StructType> s_schemaCache =
            new ConcurrentDictionary<string, StructType>();

        //private object[] _values;

        ///// <summary>
        ///// Stores the schema for a row.
        ///// </summary>
        //private StructType _schema;

        private object[] _args;

        //private List<RowConstructor> _children;

        private RowConstructor _parent;

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
        /// <returns>New RowConstructor object capturing unpickled data</returns>
        public object construct(object[] args)
        {
            if ((args.Length == 1) && (args[0] is RowConstructor))
            {
                args[0] = ((RowConstructor)args[0]).GetRow();
                return args;
            }

            return new RowConstructor(this, args);
        }

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

        private StructType GetSchema()
        {
            Debug.Assert((_args != null) && (_args.Length == 1) && (_args[0] is string));
            string schemaString = _args[0] as string;
            return s_schemaCache
                .GetOrAdd(schemaString, s => (StructType)DataType.ParseDataType(s));
        }

        internal void Reset()
        {
            s_schemaCache.Clear();
        }
    }
}
