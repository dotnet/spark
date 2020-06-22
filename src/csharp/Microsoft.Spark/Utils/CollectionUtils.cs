// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;

namespace Microsoft.Spark.Utils
{
    internal static class CollectionUtils
    {
        internal static bool ArrayEquals<T>(T[] array1, T[] array2)
        {
            return (array1?.Length == array2?.Length) &&
                ((array1 == null) || array1.SequenceEqual(array2));
        }
    }
}
