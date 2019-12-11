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
    /// Represents a row object in RDD, equivalent to GenericRow in Spark.
    /// </summary>
    public class GenericRow
    {
        /// <summary>
        /// Constructor for the GenericRow class.
        /// </summary>
        /// <param name="values">Column values for a row</param>        
        public GenericRow(object[] values)
        {
            Values = values;           
        }
       
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
        /// Checks if the given object is same as the current object.
        /// </summary>
        /// <param name="obj">Other object to compare against</param>
        /// <returns>True if the other object is equal.</returns>
        public override bool Equals(object obj) =>
            ReferenceEquals(this, obj) ||
            ((obj is GenericRow row) && Values.SequenceEqual(row.Values));

        /// <summary>
        /// Returns the hash code of the current object.
        /// </summary>
        /// <returns>The hash code of the current object</returns>
        public override int GetHashCode() => base.GetHashCode();
        
    }
}
