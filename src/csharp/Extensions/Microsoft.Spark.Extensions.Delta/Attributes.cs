using System;

namespace Microsoft.Spark.Extensions.Delta
{
    /// <summary>
    /// Custom attribute to denote the Delta Lake version in which an API is introduced.
    /// </summary>
    [AttributeUsage(AttributeTargets.All)]
    public sealed class DeltaSinceAttribute : VersionAttribute
    {
        /// <summary>
        /// Constructor for DeltaSinceAttribute class.
        /// </summary>
        /// <param name="version">Delta Lake version</param>
        public DeltaSinceAttribute(string version)
            : base(version)
        {
        }
    }
}
