// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.Spark.Utils
{
    internal class DependencyProviderUtils
    {
        internal static readonly string s_filePattern = "dependencyProviderMetadata_*";

        internal static string FindHighestFile(string path)
        {
            string[] files = Directory.GetFiles(path, s_filePattern);
            if (files.Length > 0)
            {
                Array.Sort(files);
                return files.Last();
            }

            return null;
        }

        internal static string CreateFileName(ulong number) =>
            s_filePattern.Replace("*", $"{number:D20}");

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

            public bool Equals(NuGetMetadata other)
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

            internal static Metadata Deserialize(string path)
            {
                using FileStream fileStream = File.OpenRead(path);
                var formatter = new BinaryFormatter();
                return (Metadata)formatter.Deserialize(fileStream);
            }

            internal void Serialize(string path)
            {
                using FileStream fileStream = File.OpenWrite(path);
                var formatter = new BinaryFormatter();
                formatter.Serialize(fileStream, this);
            }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                return (obj is Metadata metadata) &&
                    Equals(metadata);
            }

            public bool Equals(Metadata other)
            {
                return (other != null) &&
                    ArrayEquals(AssemblyProbingPaths, other.AssemblyProbingPaths) &&
                    ArrayEquals(NativeProbingPaths, other.NativeProbingPaths) &&
                    ArrayEquals(NuGets, other.NuGets);
            }

            private static bool ArrayEquals<T>(T[] array1, T[] array2)
            {
                return (array1?.Length == array2?.Length) &&
                    ((array1 == null) || array1.SequenceEqual(array2));
            }
        }
    }
}
