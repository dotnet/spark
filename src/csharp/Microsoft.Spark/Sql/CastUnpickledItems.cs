// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;

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
        /// <param name="unpickledItems">Unpickled items that contains simple array
        /// and array of arrays</param>
        /// <returns>Simple array after casting</returns>
        public static object[] CastToSimpleArray(object unpickledItems)
        {
            var castUnpickledItems = new List<object>();
            foreach (object[] objArr in (object[])unpickledItems)
            {
                var convertedObjArr = new List<object>();
                foreach (object obj in objArr)
                {
                    convertedObjArr.Add(
                        (obj != null && obj.GetType() == typeof(ArrayList)) ?
                        TypeConverter(obj as ArrayList) : obj);
                }
                castUnpickledItems.Add(convertedObjArr.ToArray());
            }
            return castUnpickledItems.ToArray();
        }

        /// <summary>
        /// Cast unpickledItems from arraylist to the appropriate array.
        /// </summary>
        /// <param name="unpickledItems">Unpickled items that contains array of rows</param>
        /// <returns>Array of rows after casting</returns>
        /// I will clean up the following part
        public static object[] CastToRowArray(object unpickledItems)
        {
            var castUnpickledItems = new List<object>();
            foreach (RowConstructor rowConstructor in (object[])unpickledItems)
            {
                Row row = rowConstructor.GetRow();
                var firstValue = row.Values[0] as ArrayList;
                if (firstValue[0].GetType() == typeof(RowConstructor))
                {
                    var castRowArr = new List<Row>();
                    var castObjArr = new List<object>();
                    foreach (RowConstructor rc in firstValue)
                    {
                        Row r = rc.GetRow();
                        var values = new List<object>();
                        foreach (object value in r.Values)
                        {
                            if (value != null && value.GetType() == typeof(ArrayList))
                            {
                                values.Add(TypeConverter(value as ArrayList));
                            }
                            else
                            {
                                values.Add(value);
                            }
                        }
                        castRowArr.Add(new Row(values.ToArray(), r.Schema));
                    }
                    castObjArr.Add(castRowArr.ToArray());
                    castUnpickledItems.Add(castObjArr.ToArray());
                }
                else
                {
                    var values = new object[] { TypeConverter(firstValue) };
                    castUnpickledItems.Add(new Row(values, row.Schema));
                }
            }

            return castUnpickledItems.ToArray();
        }

        /// <summary>
        /// Cast arraylist to typed array.
        /// </summary>
        /// <param name="arrList">ArrayList to be converted</param>
        /// <returns>Typed array</returns>
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
                _ when type == typeof(ArrayList) => CastArrayOfArrays(arrList),
                _ => throw new NotSupportedException(
                        string.Format("Type {0} not supported yet", type))
            };
        }

        /// <summary>
        /// Cast array of arrays.
        /// </summary>
        /// <param name="arrList">ArrayList to be converted</param>
        /// <returns>Typed array</returns>
        public static object CastArrayOfArrays(ArrayList arrList)
        {
            var convertedArray = new ArrayList();
            foreach (ArrayList al in arrList)
            {
                convertedArray.Add(TypeConverter(al));
            }
            return convertedArray.ToArray(convertedArray[0].GetType());
        }
    }
}
