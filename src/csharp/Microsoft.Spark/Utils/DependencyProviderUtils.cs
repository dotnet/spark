// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;

namespace Microsoft.Spark.Utils
{
    internal class DependencyProviderUtils
    {
        private static readonly string s_filePattern = "dependencyProviderMetadata_*";

        internal static string[] GetMetadataFiles(string path) =>
            Directory.GetFiles(path, s_filePattern);

        // Create the dependency provider metadata filename based on the number passed into the
        // function.
        // 
        // number => filename
        // 0      => dependencyProviderMetadata_0000000000000000000
        // 1      => dependencyProviderMetadata_0000000000000000001
        // ...
        // 20     => dependencyProviderMetadata_0000000000000000020
        internal static string CreateFileName(long number) =>
            s_filePattern.Replace("*", $"{number:D19}");

        [Serializable]
        internal class NuGetMetadata
        {
            public string FileName { get; set; }
            public string PackageName { get; set; }
            public string PackageVersion { get; set; }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                return (obj is NuGetMetadata nugetMetadata) &&
                    Equals(nugetMetadata);
            }

            private bool Equals(NuGetMetadata other)
            {
                return (other != null) &&
                    (FileName == other.FileName) &&
                    (PackageName == other.PackageName) &&
                    (PackageVersion == other.PackageVersion);
            }
        }

        [Serializable]
        internal class Metadata
        {
            public string[] AssemblyProbingPaths { get; set; }
            public string[] NativeProbingPaths { get; set; }
            public NuGetMetadata[] NuGets { get; set; }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                return (obj is Metadata metadata) &&
                    Equals(metadata);
            }

            internal static Metadata Deserialize(string path)
            {
                using FileStream fileStream = File.OpenRead(path);
                return BinarySerDe.Deserialize<Metadata>(fileStream);
            }

            internal void Serialize(string path)
            {
                using FileStream fileStream = File.OpenWrite(path);
                BinarySerDe.Serialize(fileStream, this);
            }

            private bool Equals(Metadata other)
            {
                return (other != null) &&
                    CollectionUtils.ArrayEquals(
                        AssemblyProbingPaths,
                        other.AssemblyProbingPaths) &&
                    CollectionUtils.ArrayEquals(NativeProbingPaths, other.NativeProbingPaths) &&
                    CollectionUtils.ArrayEquals(NuGets, other.NuGets);
            }
        }
    }
}
