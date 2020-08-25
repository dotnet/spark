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
    internal class CastUnpickledItems
    {
        /// <summary>
        /// Cast UnpickledItems from ArrayList to Array.
        /// </summary>
        /// <param name="unpickledItems">Unpickled items</param>
        /// <returns>Cast unpickled items</returns>
        public static object[] CastToArray(object unpickledItems)
        {
            var castUnpickledItems = new List<object>();
            foreach (object[] objArr in (object[])unpickledItems)
            {
                var castObjArr = new List<object>();
                ArrayList arrli = (ArrayList)objArr[0];
                if (arrli.Count == 0)
                {
                    castObjArr.Add(null);
                }
                else
                {
                    Type type = arrli[0].GetType();
                    switch (Type.GetTypeCode(type))
                    {
                        case TypeCode.Int32:
                            castObjArr.Add((int[])arrli.ToArray(typeof(int)));
                            break;
                        case TypeCode.Int64:
                            castObjArr.Add((long[])arrli.ToArray(typeof(long)));
                            break;
                        case TypeCode.Double:
                            castObjArr.Add((double[])arrli.ToArray(typeof(double)));
                            break;
                        case TypeCode.Byte:
                            castObjArr.Add((byte[])arrli.ToArray(typeof(byte)));
                            break;
                        case TypeCode.String:
                            castObjArr.Add((string[])arrli.ToArray(typeof(string)));
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
