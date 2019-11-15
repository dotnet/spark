// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Represents a row object in RDD, equivalent to GenericRowWithSchema in Spark.
    /// </summary>
    public sealed class Row
    {
        /// <summary>
        /// Constructor for the Row class.
        /// </summary>
        /// <param name="values">Column values for a row</param>
        /// <param name="schema">Schema associated with a row</param>
        public Row(object[] values, StructType schema)
        {
            Values = values;
            Schema = schema;
            var schemaColumnCount = Schema.Fields.Count;
            if (Size() != schemaColumnCount)
            {
                throw new Exception(
                    $"Column count mismatches: data:{Size()}, schema:{schemaColumnCount}");
            }

            Convert();     
        }

        /// <summary>
        /// Schema associated with this row.
        /// </summary>
        public StructType Schema { get; }

        /// <summary>
        /// Values representing this row.
        /// </summary>
        public object[] Values { get; }

        /// <summary>
        /// Returns the number of columns in this row.
        /// </summary>
        /// <returns>Number of columns in this row</returns>
        public int Size() => Values.Length;

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
        public object Get(int index)
        {
            if (index >= Size())
            {
                throw new IndexOutOfRangeException($"index ({index}) >= column counts ({Size()})");
            }
            else if (index < 0)
            {
                throw new IndexOutOfRangeException($"index ({index}) < 0)");
            }

            return Values[index];
        }

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
        public override string ToString()
        {
            var cols = new List<string>();
            foreach (object item in Values)
            {
                cols.Add(item?.ToString() ?? string.Empty);
            }

            return $"[{(string.Join(",", cols.ToArray()))}]";
        }

        /// <summary>
        /// Returns the column value at the given index, as a type T.
        /// TODO: If the original type is "long" and its value can be
        /// fit into the "int", Pickler will serialize the value as int.
        /// Since the value is boxed, <see cref="GetAs{T}(int)"/> will throw an exception.
        /// </summary>
        /// <typeparam name="T">Type to convert to</typeparam>
        /// <param name="index">Index to look up</param>
        /// <returns>A column value as a type T</returns>
        public T GetAs<T>(int index) => (T)Get(index);

        /// <summary>
        /// Returns the column value whose column name is given, as a type T.
        /// TODO: If the original type is "long" and its value can be
        /// fit into the "int", Pickler will serialize the value as int.
        /// Since the value is boxed, <see cref="GetAs{T}(string)"/> will throw an exception.
        /// </summary>
        /// <typeparam name="T">Type to convert to</typeparam>
        /// <param name="columnName">Column name to look up</param>
        /// <returns>A column value as a type T</returns>
        public T GetAs<T>(string columnName) => (T)Get(columnName);

        /// <summary>
        /// Checks if the given object is same as the current object.
        /// </summary>
        /// <param name="obj">Other object to compare against</param>
        /// <returns>True if the other object is equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj is Row otherRow)
            {
                return Values.SequenceEqual(otherRow.Values) &&
                    Schema.Equals(otherRow.Schema);
            }

            return false;
        }

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
            foreach (StructField field in Schema.Fields)
            {
                if (field.DataType is ArrayType)
                {
                    throw new NotImplementedException();
                }
                else if (field.DataType is MapType)
                {
                    throw new NotImplementedException();
                }
                else if (field.DataType is DecimalType)
                {
                    throw new NotImplementedException();
                }
                else if (field.DataType is DateType)
                {
                    throw new NotImplementedException();
                }
            }
        }
    }
}
