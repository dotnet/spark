// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Spark
{
    /// <summary>
    /// Extra functions available on RDDs of (key, value) pairs through extension methods.
    /// </summary>
    internal static class PairRDDFunctions
    {
        /// <summary>
        /// Returns the key-value pairs in this RDD as a dictionary.
        /// </summary>
        /// <typeparam name="TKey">Type of the key</typeparam>
        /// <typeparam name="TValue">Type of the value</typeparam>
        /// <param name="self">RDD object to apply</param>
        /// <returns>Dictionary of RDD content</returns>
        public static IDictionary<TKey, TValue> CollectAsMap<TKey, TValue>(
            this RDD<Tuple<TKey, TValue>> self)
        {
            return self.Collect().ToDictionary(tuple => tuple.Item1, tuple => tuple.Item2);
        }

        /// <summary>
        /// Return an RDD with the keys of each tuple.
        /// </summary>
        /// <typeparam name="TKey">Type of the key</typeparam>
        /// <typeparam name="TValue">Type of the value</typeparam>
        /// <param name="self">RDD object to apply</param>
        /// <returns>RDD with the keys of each tuple</returns>
        public static RDD<TKey> Keys<TKey, TValue>(this RDD<Tuple<TKey, TValue>> self)
        {
            return self.Map(tuple => tuple.Item1);
        }

        /// <summary>
        /// Return an RDD with the values of each tuple.
        /// </summary>
        /// <typeparam name="TKey">Type of the key</typeparam>
        /// <typeparam name="TValue">Type of the value</typeparam>
        /// <param name="self">RDD object to apply</param>
        /// <returns>RDD with the values of each tuple</returns>
        public static RDD<TValue> Values<TKey, TValue>(this RDD<Tuple<TKey, TValue>> self)
        {
            return self.Map(tuple => tuple.Item2);
        }
    }
}
