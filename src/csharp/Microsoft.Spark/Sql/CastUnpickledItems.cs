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
        /// Cast UnpickledItems from ArrayList to Array.
        /// </summary>
        /// <param name="unpickledItems">Unpickled items</param>
        /// <returns>Array after casting</returns>
        public static object[] CastToArray(object unpickledItems)
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
                        default:
                            throw new NotSupportedException(
                                string.Format("Type {0} not supported yet", type));
                    }
                }
                castUnpickledItems.Add(castObjArr.ToArray());
            }
            return castUnpickledItems.ToArray();
        }
    }
}
