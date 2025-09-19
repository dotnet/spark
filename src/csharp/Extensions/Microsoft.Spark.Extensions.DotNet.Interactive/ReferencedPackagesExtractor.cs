using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.CSharp;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    internal class ReferencedPackagesExtractor
    {
        private readonly CSharpKernel _kernel;

        public ReferencedPackagesExtractor(CSharpKernel kernel)
        {
            _kernel = kernel;
        }

        /// <summary>
        /// Gets collection of <see cref="ResolvedPackageReference"/>, extracted from <see cref="CSharpKernel"/> using reflection.
        /// </summary>
        /// <remarks>
        /// This implementation uses a fragile approach, relying on reflection to access
        /// the private "_disposables" field and internal "PackageRestoreContext" type within
        /// the kernel. This is necessary because a public API for retrieving this
        /// information is not available. This code is susceptible to breaking in future
        /// versions of the Microsoft.DotNet.Interactive library if its internal
        /// structure changes.
        /// https://github.com/dotnet/spark/discussions/1179
        /// </remarks>
        internal virtual IEnumerable<ResolvedPackageReference> ResolvedPackageReferences
        {
            get
            {
                var disposablesField = typeof(Microsoft.DotNet.Interactive.Kernel).GetField("_disposables", BindingFlags.Instance | BindingFlags.NonPublic);
                var disposables = disposablesField?.GetValue(_kernel) as IEnumerable<IDisposable>;

                if (disposables is null)
                {
                    throw new Exception("Failed to retrieve referenced packages from kernel, try using older version of Dotnet.Interactive");
                }

                // Find the PackageRestoreContext instance in internal collection of disposables
                var restoreContext = disposables
                    .FirstOrDefault(d => d.GetType().FullName == "Microsoft.DotNet.Interactive.PackageManagement.PackageRestoreContext");

                if (restoreContext is null)
                {
                    throw new Exception("PackageRestoreContext was not found in the kernel, try using older version of Dotnet.Interactive");
                }

                // Extract the ResolvedPackageReferences property (internal)
                var resolvedPackagesProp = restoreContext
                    .GetType()
                    .GetProperty("ResolvedPackageReferences", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);

                return resolvedPackagesProp?.GetValue(restoreContext) as IEnumerable<ResolvedPackageReference>;
            }
        }
    }
}
