// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.Utility;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    internal class PackageResolver : IPackageResolver
    {
        private readonly HashSet<string> _filesCopied = new HashSet<string>();

        /// <summary>
        /// Return the delta of the list of packages that have been introduced
        /// since the last call.
        /// </summary>
        /// <returns>The delta of the list of packages.</returns>
        public IEnumerable<ResolvedNuGetPackage> GetPackagesToCopy()
        {
            PackageRestoreContext restoreContext =
                ((ISupportNuget)KernelInvocationContext.Current.HandlingKernel)
                .PackageRestoreContext;
            IEnumerable<ResolvedPackageReference> packages =
                restoreContext.ResolvedPackageReferences;
            foreach (ResolvedPackageReference package in packages)
            {
                IEnumerable<FileInfo> files =
                    package.PackageRoot.EnumerateFiles("*.nupkg", SearchOption.AllDirectories);

                foreach (FileInfo file in files)
                {
                    if (_filesCopied.Contains(file.Name))
                    {
                        continue;
                    }

                    _filesCopied.Add(file.Name);
                    yield return new ResolvedNuGetPackage
                    {
                        ResolvedPackage = package, NuGetFile = file
                    };
                }
            }
        }
    }
}
