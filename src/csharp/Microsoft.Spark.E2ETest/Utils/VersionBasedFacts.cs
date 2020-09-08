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
                Skip = $"Ignore on Spark version ({SparkSettings.Version}) < {version}";
            }
        }
    }

    public sealed class SkipIfSparkVersionIsGreaterOrEqualTo : FactAttribute
    {
        public SkipIfSparkVersionIsGreaterOrEqualTo(string version)
        {
            if (SparkSettings.Version >= new Version(version))
            {
                Skip = $"Ignore on Spark version ({SparkSettings.Version}) >= {version}";
            }
        }
    }

    // Skip if the spark version is not in range [minVersion, maxVersion).
    public sealed class SkipIfSparkVersionIsNotInRange : FactAttribute
    {
        public SkipIfSparkVersionIsNotInRange(string minInclusive, string maxExclusive)
        {
            if (SparkSettings.Version < new Version(minInclusive) ||
                SparkSettings.Version >= new Version(maxExclusive))
            {
                Skip = $"Ignore on Spark version ({SparkSettings.Version}) not in range of " +
                    $"[{minInclusive}, {maxExclusive})";
            }
        }
    }
}
