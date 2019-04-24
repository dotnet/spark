// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// SaveMode is used to specify the expected behavior of saving a DataFrame to a data source.
    /// </summary>
    public enum SaveMode
    {
        /// <summary>
        /// Append mode means that when saving a DataFrame to a data source, if data/table already
        /// exists, contents of the DataFrame are expected to be appended to existing data.
        /// </summary>
        Append,

        /// <summary>
        /// Overwrite mode means that when saving a DataFrame to a data source,
        /// if data/table already exists, existing data is expected to be overwritten by the
        /// contents of the DataFrame.
        /// </summary>
        Overwrite,

        /// <summary>
        /// ErrorIfExists mode means that when saving a DataFrame to a data source, if data already
        /// exists, an exception is expected to be thrown.
        /// </summary>
        ErrorIfExists,

        /// <summary>
        /// Ignore mode means that when saving a DataFrame to a data source, if data already exists,
        /// the save operation is expected to not save the contents of the DataFrame and to not
        /// change the existing data.
        /// </summary>
        Ignore
    }
}
