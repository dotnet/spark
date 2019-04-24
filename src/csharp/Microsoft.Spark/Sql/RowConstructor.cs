// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
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
        /// Schema of the rows being received. Note that this is thread local variable
        /// because one RowConstructor object is registered to the Unpickler and there
        /// could be multiple threads unpickling the data using the same object registered.
        /// </summary>
        [ThreadStatic]
        private static StructType s_currentSchema;

        /// <summary>
        /// Stores values passed from construct().
        /// </summary>
        private object[] _values;

        /// <summary>
        /// Stores the schema for a row.
        /// </summary>
        private StructType _schema;

        /// <summary>
        /// Returns a string representation of this object.
        /// </summary>
        /// <returns>A string representation of this object</returns>
        public override string ToString()
        {
            return string.Format("{{{0}}}", string.Join(",", _values));
        }

        /// <summary>
        /// Used by Unpickler to pass unpickled data for handling.
        /// </summary>
        /// <param name="args">Unpickled data</param>
        /// <returns>New RowConstructor object capturing unpickled data</returns>
        public object construct(object[] args)
        {
            // Every first call to construct() contains the schema data. When
            // a new RowConstructor object is returned from this function,
            // construct() is called on the returned object with the actual
            // row data.

            // Cache the schema to avoid parsing it for each construct() call.
            if (s_currentSchema is null)
            {
                s_currentSchema = (StructType)DataType.ParseDataType((string)args[0]);
            }

            // Note that on the first call, _values will be set to schema passed in,
            // but the subsequent construct() call on this object will replace
            // _values with the actual row data.
            return new RowConstructor { _values = args, _schema = s_currentSchema };
        }

        /// <summary>
        /// Construct a Row object from unpickled data.
        /// Resets the cached row schema.
        /// </summary>
        /// <returns>A row object with unpickled data</returns>
        public Row GetRow()
        {
            // Reset the current schema to ensure that the subsequent construct()
            // will use the correct schema if rows from different tables are being
            // unpickled. This means that if pickled data is received per row, the
            // JSON schema will be parsed for each row. In Spark, rows are batched
            // such that the schema will be parsed once for a batch.
            // One optimization is to introduce Reset() to reset the s_currentSchema,
            // but this has a burden of remembering to call Reset() on the user side,
            // thus not done.
            s_currentSchema = null;

            return new Row(_values, _schema);
        }
    }
}
