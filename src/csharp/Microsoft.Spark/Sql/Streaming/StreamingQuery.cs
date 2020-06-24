// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Streaming
{
    /// <summary>
    /// A handle to a query that is executing continuously in the background as new data arrives.
    /// </summary>
    public sealed class StreamingQuery : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal StreamingQuery(JvmObjectReference jvmObject) => _jvmObject = jvmObject;

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Returns the user-specified name of the query, or null if not specified.
        /// </summary>
        public string Name => (string)_jvmObject.Invoke("name");

        /// <summary>
        /// Returns true if this query is actively running.
        /// </summary>
        /// <returns>True if this query is actively running</returns>
        public bool IsActive() => (bool)_jvmObject.Invoke("isActive");

        /// <summary>
        /// Waits for the termination of this query, either by Stop() or by an exception.
        /// </summary>
        public void AwaitTermination() => _jvmObject.Invoke("awaitTermination");

        /// <summary>
        /// Returns true if this query is terminated within the timeout in milliseconds.
        /// </summary>
        /// <remarks>
        /// If the query has terminated, then all subsequent calls to this method will either
        /// return true immediately (if the query was terminated by Stop()), or throw an
        /// exception immediately (if the query has terminated with an exception).
        /// </remarks>
        /// <param name="timeoutMs">Timeout value in milliseconds</param>
        /// <returns>true if this query is terminated within timeout</returns>
        public bool AwaitTermination(long timeoutMs) =>
            (bool)_jvmObject.Invoke("awaitTermination", timeoutMs);

        /// <summary>
        /// Stops the execution of this query if it is running. This method blocks until the
        /// threads performing execution stop.
        /// </summary>
        public void Stop() => _jvmObject.Invoke("stop");

        /// <summary>
        /// Prints the physical plan to the console for debugging purposes.
        /// </summary>
        /// <param name="extended">Whether to do extended explain or not</param>
        public void Explain(bool extended = false) => _jvmObject.Invoke("explain", extended);
    }
}
