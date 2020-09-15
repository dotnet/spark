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
                castUnpickledItems.Add(TypeConverter(objArr[0] as ArrayList));
            }
            return castUnpickledItems.ToArray();
        }

        /// <summary>
        /// Cast unpickledItems from arraylist to the appropriate array.
        /// </summary>
        /// <param name="unpickledItems">Unpickled items that contains array of rows</param>
        /// <returns>Array of rows after casting</returns>
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
                        castRowArr.Add(rc.GetRow());
                    }
                    castObjArr.Add(castRowArr.ToArray());
                    castUnpickledItems.Add(castObjArr.ToArray());
                }
                else
                {
                    var values = TypeConverter(firstValue);
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
        public static object[] TypeConverter(ArrayList arrList)
        {
            var castObjArr = new List<object>();
            Type type = arrList[0].GetType();
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Int32:
                    castObjArr.Add((int[])arrList.ToArray(typeof(int)));
                    break;
                case TypeCode.Int64:
                    castObjArr.Add((long[])arrList.ToArray(typeof(long)));
                    break;
                case TypeCode.Double:
                    castObjArr.Add((double[])arrList.ToArray(typeof(double)));
                    break;
                case TypeCode.Byte:
                    castObjArr.Add((byte[])arrList.ToArray(typeof(byte)));
                    break;
                case TypeCode.String:
                    castObjArr.Add((string[])arrList.ToArray(typeof(string)));
                    break;
                case TypeCode.Object:
                    Type t = ((ArrayList)arrList[0])[0].GetType();
                    int length = arrList.Count;
                    Array arr = Array.CreateInstance(t, length);
                    for (int i = 0; i < length; ++i)
                    {
                        arr.SetValue(TypeConverter(arrList[i] as ArrayList)[0], i);
                    }
                    castObjArr.Add(arr);
                    break;
                default:
                    throw new NotSupportedException(
                        string.Format("Type {0} not supported yet", type));
            }
            return castObjArr.ToArray();
        }
    }
}
