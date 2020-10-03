// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using Microsoft.DotNet.Interactive;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    internal class ResolvedNuGetPackage
    {
        public ResolvedPackageReference ResolvedPackage { get; set; }
        public DirectoryInfo PackageRootDirectory { get; set; }
        public FileInfo NuGetFile { get; set; }
    }
}
