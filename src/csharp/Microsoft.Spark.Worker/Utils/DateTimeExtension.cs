// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Worker.Utils
{
    internal static class DateTimeExtension
    {
        internal static long ToUnixTime(this DateTime dt)
        {
            var unixTimeEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return (long)(dt - unixTimeEpoch).TotalMilliseconds;
        }
    }
}
