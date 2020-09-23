// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// Converts one type to another.
    /// </summary>
    internal static class TypeConverter
    {
        /// <summary>
        /// Convert obj to type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">Type to convert to</typeparam>
        /// <param name="obj">The object to convert</param>
        /// <returns>Converted object.</returns>
        internal static T Convert<T>(object obj) => obj is T t ? t : (T)Convert(obj, typeof(T));

        private static object Convert(object obj, Type toType)
        {
            if ((obj is ArrayList arrayList) && toType.IsArray)
            {
                return ConvertArrayList(arrayList, toType);
            }
            else if ((obj is Hashtable hashtable) && toType.IsGenericType &&
                (toType.GetGenericTypeDefinition() == typeof(Dictionary<,>)))
            {
                return ConvertHashtable(hashtable, toType);
            }

            return obj;
        }

        private static object ConvertArrayList(ArrayList arrayList, Type type)
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

        private static object ConvertHashtable(Hashtable hashtable, Type type)
        {
            Type[] genericTypes = type.GetGenericArguments();
            IDictionary dict =
                (IDictionary)Activator.CreateInstance(type, new object[] { hashtable.Count });
            foreach (DictionaryEntry entry in hashtable)
            {
                dict[Convert(entry.Key, genericTypes[0])] = Convert(entry.Value, genericTypes[1]);
            }

            return dict;
        }
    }
}
