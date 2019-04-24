// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.Sql.Streaming
{
    /// <summary>
    /// Output modes for specifying how data of a streaming DataFrame is written to a
    /// streaming sink.
    /// </summary>
    public enum OutputMode
    {
        /// <summary>
        /// OutputMode in which only the new rows in the streaming DataFrame/Dataset will be
        /// written to the sink. This output mode can be only be used in queries that do not
        /// contain any aggregation.
        /// </summary>
        Append,

        /// <summary>
        /// OutputMode in which all the rows in the streaming DataFrame/Dataset will be written
        /// to the sink every time these is some updates. This output mode can only be used in
        /// queries that contain aggregations.
        /// </summary>
        Complete,

        /// <summary>
        /// OutputMode in which only the rows in the streaming DataFrame/Dataset that were updated
        /// will be written to the sink every time these is some updates. If the query doesn't
        /// contain aggregations, it will be equivalent to Append mode.
        /// </summary>
        Update
    }
}
