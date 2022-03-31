// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Represents a row object in RDD, equivalent to GenericRowWithSchema in Spark.
    /// </summary>
    public sealed class Row
    {
        private readonly GenericRow _genericRow;

        /// <summary>
        /// Constructor for the Row class.
        /// </summary>
        /// <param name="values">Column values for a row</param>
        /// <param name="schema">Schema associated with a row</param>
        internal Row(object[] values, StructType schema)
        {
            _genericRow = new GenericRow(values);
            Schema = schema;

            int schemaColumnCount = Schema.Fields.Count;
            if (Size() != schemaColumnCount)
            {
                throw new Exception(
                    $"Column count mismatches: data:{Size()}, schema:{schemaColumnCount}");
            }

            Convert();
        }

        /// <summary>
        /// Constructor for the schema-less Row class used for chained UDFs.
        /// </summary>
        /// <param name="genericRow">GenericRow object</param>
        internal Row(GenericRow genericRow)
        {
            _genericRow = genericRow;
        }

        /// <summary>
        /// Returns schema-less Row which can happen within chained UDFs (same behavior as PySpark).
        /// </summary>
        /// <remarks>
        /// The use of this conversion operator is discouraged except for the UDF that returns
        /// a Row object.
        /// </remarks>
        /// <returns>schema-less Row</returns>
        public static implicit operator Row(GenericRow genericRow)
        {
            return new Row(genericRow);
        }

        /// <summary>
        /// Schema associated with this row.
        /// </summary>
        public StructType Schema { get; }

        /// <summary>
        /// Values representing this row.
        /// </summary>
        public object[] Values => _genericRow.Values;

        /// <summary>
        /// Returns the number of columns in this row.
        /// </summary>
        /// <returns>Number of columns in this row</returns>
        public int Size() => _genericRow.Size();

        /// <summary>
        /// Returns the column value at the given index.
        /// </summary>
        /// <param name="index">Index to look up</param>
        /// <returns>A column value</returns>
        public object this[int index] => Get(index);

        /// <summary>
        /// Returns the column value at the given index.
        /// </summary>
        /// <param name="index">Index to look up</param>
        /// <returns>A column value</returns>
        public object Get(int index) => _genericRow.Get(index);
        /// <summary>
        /// Returns the column value whose column name is given.
        /// </summary>
        /// <param name="columnName">Column name to look up</param>
        /// <returns>A column value</returns>
        public object Get(string columnName) =>
            Get(Schema.Fields.FindIndex(f => f.Name == columnName));

        /// <summary>
        /// Returns the string version of this row.
        /// </summary>
        /// <returns>String version of this row</returns>
        public override string ToString() => _genericRow.ToString();

        /// <summary>
        /// Returns the column value at the given index, as a type T.
        /// TODO: If the original type is "long" and its value can be
        /// fit into the "int", Pickler will serialize the value as int.
        /// Since the value is boxed, <see cref="GetAs{T}(int)"/> will throw an exception.
        /// </summary>
        /// <typeparam name="T">Type to convert to</typeparam>
        /// <param name="index">Index to look up</param>
        /// <returns>A column value as a type T</returns>
        public T GetAs<T>(int index) => TypeConverter.ConvertTo<T>(Get(index));

        /// <summary>
        /// Returns the column value whose column name is given, as a type T.
        /// TODO: If the original type is "long" and its value can be
        /// fit into the "int", Pickler will serialize the value as int.
        /// Since the value is boxed, <see cref="GetAs{T}(string)"/> will throw an exception.
        /// </summary>
        /// <typeparam name="T">Type to convert to</typeparam>
        /// <param name="columnName">Column name to look up</param>
        /// <returns>A column value as a type T</returns>
        public T GetAs<T>(string columnName) => TypeConverter.ConvertTo<T>(Get(columnName));

        /// <summary>
        /// Checks if the given object is same as the current object.
        /// </summary>
        /// <param name="obj">Other object to compare against</param>
        /// <returns>True if the other object is equal.</returns>
        public override bool Equals(object obj) =>
            ReferenceEquals(this, obj) ||
            ((obj is Row row) && _genericRow.Equals(row._genericRow)) && Schema.Equals(row.Schema);

        /// <summary>
        /// Returns the hash code of the current object.
        /// </summary>
        /// <returns>The hash code of the current object</returns>
        public override int GetHashCode() => base.GetHashCode();

        /// <summary>
        /// Converts the values to .NET values. Currently, only the simple types such as
        /// int, string, etc. are supported (which are already converted correctly by
        /// the Pickler). Note that explicit type checks against the schema are not performed.
        /// </summary>
        private void Convert()
        {
            for (int i = 0; i < Size(); ++i)
            {
                DataType dataType = Schema.Fields[i].DataType;
                if (dataType.NeedConversion())
                {
                    Values[i] = dataType.FromInternal(Values[i]);
                }
            }
        }
    }
}
