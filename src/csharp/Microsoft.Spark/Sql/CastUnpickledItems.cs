// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Cast UnpickledItems.
    /// </summary>
    internal static class CastUnpickledItems
    {
        /// <summary>
        /// Cast unpickledItems from arraylist to the appropriate array.
        /// </summary>
        /// <param name="unpickledItems">Unpickled objects to be cast as necessary.</param>
        /// <returns>Unpickled objects after casting.</returns>
        public static object[] Cast(object unpickledItems)
        {
            var castUnpickledItems = new List<object>();
            foreach (object obj in (object[])unpickledItems)
            {
                castUnpickledItems.Add(
                    obj.GetType() == typeof(RowConstructor) ?
                    CastRow(obj) as Row:
                    CastArray(obj));
            }

            return castUnpickledItems.ToArray();
        }

        /// <summary>
        /// Cast simple array and array of arrays.
        /// </summary>
        /// <param name="obj">object to be cast as necessary.</param>
        /// <returns>Typed array after casting.</returns>
        public static object CastArray(object obj)
        {
            if (obj is object[] objArr)
            {
                return objArr.Select(x => CastHelper(x)).ToArray();
            }

            // Array of Arrays.
            var convertedArray = new ArrayList();
            foreach (ArrayList arrList in obj as ArrayList)
            {
                convertedArray.Add(TypeConverter(arrList));
            }
            return convertedArray.ToArray(convertedArray[0].GetType());
        }

        /// <summary>
        /// Cast row.
        /// </summary>
        /// <param name="obj">object to be cast as necessary.</param>
        /// <returns>Row after casting.</returns>
        public static object CastRow(object obj)
        {
            if (obj is RowConstructor rowConstructor)
            {
                Row row = rowConstructor.GetRow();
                object[] values = (row.Values).Select(x => CastHelper(x)).ToArray();
                return new Row(values, row.Schema);
            }

            // Array of rows
            var convertedRow = new List<Row>();
            foreach (RowConstructor rc in obj as ArrayList)
            {
                convertedRow.Add(CastRow(rc) as Row);
            }
            return convertedRow.ToArray();
        }

        /// <summary>
        /// Helper function to decide and cast if inputs need to be cast.
        /// </summary>
        /// <param name="obj">object to be cast.</param>
        /// <returns>Original object or cast object</returns>
        public static object CastHelper(object obj)
        {
            return obj != null && obj is ArrayList arrayList ?
                TypeConverter(arrayList) : obj;
        }

        /// <summary>
        /// Cast arraylist to typed array.
        /// </summary>
        /// <param name="arrList">ArrayList to be converted.</param>
        /// <returns>Typed array.</returns>
        public static object TypeConverter(ArrayList arrList)
        {
            Type type = arrList[0].GetType();
            return type switch
            {
                _ when type == typeof(int) => (int[])arrList.ToArray(typeof(int)),
                _ when type == typeof(long) => (long[])arrList.ToArray(typeof(long)),
                _ when type == typeof(double) => (double[])arrList.ToArray(typeof(double)),
                _ when type == typeof(byte) => (byte[])arrList.ToArray(typeof(byte)),
                _ when type == typeof(string) => (string[])arrList.ToArray(typeof(string)),
                _ when type == typeof(ArrayList) => CastArray(arrList),
                _ when type == typeof(RowConstructor) => CastRow(arrList),
                _ => throw new NotSupportedException(
                        string.Format("Type {0} not supported yet", type))
            };
        }
    }
}
