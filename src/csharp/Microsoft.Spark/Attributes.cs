// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark
{
    /// <summary>
    /// Base class for custom attributes that involve the Spark version.
    /// </summary>
    public abstract class VersionAttribute : Attribute
    {
        /// <summary>
        /// Constructor for VersionAttribute class.
        /// </summary>
        /// <param name="version">Spark version</param>
        protected VersionAttribute(string version)
        {
            Version = new Version(version);
        }

        /// <summary>
        /// Returns the Spark version.
        /// </summary>
        public Version Version { get; }
    }

    /// <summary>
    /// Custom attribute to denote the Spark version in which an API is introduced.
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class SinceAttribute : VersionAttribute
    {
        /// <summary>
        /// Constructor for SinceAttribute class.
        /// </summary>
        /// <param name="version">Spark version</param>
        public SinceAttribute(string version)
            : base(version)
        {
        }
    }

    /// <summary>
    /// Custom attribute to denote the Spark version in which an API is removed.
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class RemovedAttribute : VersionAttribute
    {
        /// <summary>
        /// Constructor for RemovedAttribute class.
        /// </summary>
        /// <param name="version">Spark version</param>
        public RemovedAttribute(string version)
            : base(version)
        {
        }
    }

    /// <summary>
    /// Custom attribute to denote the Spark version in which an API is deprecated.
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class DeprecatedAttribute : VersionAttribute
    {
        /// <summary>
        /// Constructor for DeprecatedAttribute class.
        /// </summary>
        /// <param name="version">Spark version</param>
        public DeprecatedAttribute(string version)
            : base(version)
        {
        }
    }
}
