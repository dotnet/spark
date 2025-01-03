// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;

namespace Microsoft.Spark.E2ETest
{
    /// <summary>
    /// TestEnvironment provides functionalities related to E2E test environment.
    /// </summary>
    internal static class TestEnvironment
    {
        private static string s_resourceDirectory;
        internal static string ResourceDirectory
        {
            get
            {
                if (s_resourceDirectory is null)
                {
                    s_resourceDirectory =
                        Path.Combine(
                            AppDomain.CurrentDomain.BaseDirectory,
                            "Resources")
                        + Path.DirectorySeparatorChar;
                }

                return s_resourceDirectory;
            }
        }
    }
}
