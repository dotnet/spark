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
        /// <param name="unpickledItems">Unpickled items that contains simple array</param>
        /// <returns>Simple array after casting</returns>
        public static object[] CastToSimpleArray(object unpickledItems)
        {
            var castUnpickledItems = new List<object>();
            foreach (object[] objArr in (object[])unpickledItems)
            {
                var castObjArr = new List<object>();
                var arrList = (ArrayList)objArr[0];
                if (arrList.Count == 0)
                {
                    castObjArr.Add(null);
                }
                else
                {
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
                            switch (Type.GetTypeCode(t))
                            {
                                case TypeCode.Int32:
                                    var intArr = new List<int[]>();
                                    foreach (ArrayList al in arrList)
                                    {
                                        intArr.Add((int[])al.ToArray(typeof(int)));
                                    }
                                    castObjArr.Add(intArr.ToArray());
                                    break;
                                case TypeCode.Int64:
                                    var longArr = new List<long[]>();
                                    foreach (ArrayList al in arrList)
                                    {
                                        longArr.Add((long[])al.ToArray(typeof(long)));
                                    }
                                    castObjArr.Add(longArr.ToArray());
                                    break;
                                case TypeCode.Double:
                                    var doubleArr = new List<double[]>();
                                    foreach (ArrayList al in arrList)
                                    {
                                        doubleArr.Add((double[])al.ToArray(typeof(double)));
                                    }
                                    castObjArr.Add(doubleArr.ToArray());
                                    break;
                                case TypeCode.Byte:
                                    var byteArr = new List<byte[]>();
                                    foreach (ArrayList al in arrList)
                                    {
                                        byteArr.Add((byte[])al.ToArray(typeof(byte)));
                                    }
                                    castObjArr.Add(byteArr.ToArray());
                                    break;
                                case TypeCode.String:
                                    var stringArr = new List<string[]>();
                                    foreach (ArrayList al in arrList)
                                    {
                                        stringArr.Add((string[])al.ToArray(typeof(string)));
                                    }
                                    castObjArr.Add(stringArr.ToArray());
                                    break;
                                default:
                                    throw new NotSupportedException(
                                        string.Format("Array of type {0} not supported yet", t));
                            }
                            break;
                        default:
                            throw new NotSupportedException(
                                string.Format("Type {0} not supported yet", type));
                    }
                }
                castUnpickledItems.Add(castObjArr.ToArray());
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
                var castObjArr = new List<object>();
                Row row = (rowConstructor as RowConstructor).GetRow();
                var castRowArr = new List<Row>();
                foreach (RowConstructor rc in (ArrayList)row.Values[0])
                {
                    castRowArr.Add(rc.GetRow());
                }
                castObjArr.Add(castRowArr.ToArray());
                castUnpickledItems.Add(castObjArr.ToArray());
            }
            return castUnpickledItems.ToArray();
        }
    }
}
