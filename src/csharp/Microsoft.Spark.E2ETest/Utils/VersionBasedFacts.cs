// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Xunit;

namespace Microsoft.Spark.E2ETest.Utils
{
    public sealed class SkipIfSparkVersionIsLessThan : FactAttribute
    {
        public SkipIfSparkVersionIsLessThan(string version)
        {
            if (SparkSettings.Version < new Version(version))
            {
                Skip = $"Ignore on Spark version ({SparkSettings.Version}) <= {version}";
            }
        }
    }
}
