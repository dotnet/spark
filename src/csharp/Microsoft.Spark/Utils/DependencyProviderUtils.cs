// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Utils
{
    internal class DependencyProviderUtils
    {
        internal static string FilePattern { get; } = "dependencyProviderMetadata_*";

        [Serializable]
        internal class NuGetMetadata
        {
            public string FileName { get; set; }
            public string PackageName { get; set; }
            public string PackageVersion { get; set; }
        }

        [Serializable]
        internal class Metadata
        {
            public string[] AssemblyProbingPaths { get; set; }
            public string[] NativeProbingPaths { get; set; }
            public NuGetMetadata[] NuGets { get; set; }
        }
    }
}
