// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// Supports converting <see cref="ArrayList"/> to a typed <see cref="Array"/> and
    /// <see cref="Hashtable"/> to a typed <see cref="Dictionary{TKey, TValue}"/>.
    /// </summary>
    internal static class TypeConverter
    {
        /// <summary>
        /// Convert obj to type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">Type to convert to</typeparam>
        /// <param name="obj">The object to convert</param>
        /// <returns>Converted object.</returns>
        internal static T ConvertTo<T>(object obj) => (T)Convert(obj, typeof(T));

        private static object Convert(object obj, Type toType)
        {
            if ((obj is ArrayList arrayList) && toType.IsArray)
            {
                return ConvertToArray(arrayList, toType);
            }
            else if ((obj is Hashtable hashtable) && toType.IsGenericType &&
                (toType.GetGenericTypeDefinition() == typeof(Dictionary<,>)))
            {
                return ConvertToDictionary(hashtable, toType);
            }
            // Fails to convert int to long otherwise
            else if (toType.IsPrimitive)
            {
                return System.Convert.ChangeType(obj, toType);
            }

            return obj;
        }

        private static object ConvertToArray(ArrayList arrayList, Type type)
        {
            Type elementType = type.GetElementType();
            int length = arrayList.Count;
            Array convertedArray = Array.CreateInstance(elementType, length);
            for (int i = 0; i < length; ++i)
            {
                convertedArray.SetValue(Convert(arrayList[i], elementType), i);
            }

            return convertedArray;
        }

        private static object ConvertToDictionary(Hashtable hashtable, Type type)
        {
            Type[] genericTypes = type.GetGenericArguments();
            var dict =
                (IDictionary)Activator.CreateInstance(type, new object[] { hashtable.Count });
            foreach (DictionaryEntry entry in hashtable)
            {
                dict[Convert(entry.Key, genericTypes[0])] = Convert(entry.Value, genericTypes[1]);
            }

            return dict;
        }
    }
}
