// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Extensions.Delta
{
    /// <summary>
    /// Custom attribute to denote the Delta Lake version in which an API is introduced.
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class DeltaLakeSinceAttribute : VersionAttribute
    {
        /// <summary>
        /// Constructor for DeltaLakeSinceAttribute class.
        /// </summary>
        /// <param name="version">Delta Lake version</param>
        public DeltaLakeSinceAttribute(string version)
            : base(version)
        {
        }
    }
}
