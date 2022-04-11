// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Internal.Java.Util;

namespace System
{
    /// <summary>
    /// ArrayExtensions host custom extension methods for the
    /// dotnet base class array T[].
    /// </summary>
    public static class ArrayExtensions
    {
        /// <summary>
        /// A custom extension method that helps transform from dotnet
        /// array of type T to java.util.ArrayList.
        /// </summary>
        /// <param name="array">an array instance</param>
        /// <typeparam name="T">elements type of param array</typeparam>
        /// <returns><see cref="ArrayList"/></returns>
        internal static ArrayList ToJavaArrayList<T>(this T[] array)
        {
            var arrayList = new ArrayList(SparkEnvironment.JvmBridge);
            foreach (T item in array)
            {
                arrayList.Add(item);
            }
            return arrayList;
        }
    }
}
