using System.Collections.Generic;
using Microsoft.DotNet.Interactive;
using Microsoft.DotNet.Interactive.Utility;

namespace Microsoft.Spark.Extensions.DotNet.Interactive
{
    internal class PackageRestoreContextWrapper
    {
        internal virtual IEnumerable<ResolvedPackageReference> ResolvedPackageReferences =>
            ((ISupportNuget)KernelInvocationContext.Current.HandlingKernel)
            .PackageRestoreContext
            .ResolvedPackageReferences;
    }
}
