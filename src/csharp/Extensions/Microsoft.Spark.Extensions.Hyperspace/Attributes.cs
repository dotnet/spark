// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Extensions.Hyperspace
{
    /// <summary>
    /// Custom attribute to denote the Hyperspace version in which an API is introduced.
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class HyperspaceSinceAttribute : VersionAttribute
    {
        /// <summary>
        /// Constructor for HyperspaceSinceAttribute class.
        /// </summary>
        /// <param name="version">Hyperspace version</param>
        public HyperspaceSinceAttribute(string version)
            : base(version)
        {
        }
    }
}
