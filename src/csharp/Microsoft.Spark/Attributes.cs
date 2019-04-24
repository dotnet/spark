// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark
{
    /// <summary>
    /// Custom attribute to denote the Spark version in which an API is introduced.
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class SinceAttribute : Attribute
    {
        /// <summary>
        /// Constructor for SinceAttribute class.
        /// </summary>
        /// <param name="version">Spark version</param>
        public SinceAttribute(string version)
        {
            Version = version;
        }

        /// <summary>
        /// Returns the Spark version.
        /// </summary>
        public string Version { get; }
    }
}
