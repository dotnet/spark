// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// Various environment utility methods.
    /// </summary>
    internal static class EnvironmentUtils
    {
        internal static bool GetEnvironmentVariableAsBool(string name)
        {
            string str = Environment.GetEnvironmentVariable(name);
            if (string.IsNullOrEmpty(str))
            {
                return false;
            }

            switch (str.ToLowerInvariant())
            {
                case "true":
                case "1":
                case "yes":
                    return true;
                case "false":
                case "0":
                case "no":
                    return false;
                default:
                    return false;
            }
        }
    }
}
