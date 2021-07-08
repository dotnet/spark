// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Internal.Scala;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Streaming
{
    /// <summary>
    /// A handle to a query that is executing continuously in the background as new data arrives.
    /// </summary>
    public sealed class StreamingQuery : IJvmObjectReferenceProvider
    {
        internal StreamingQuery(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Returns the user-specified name of the query, or null if not specified.
        /// </summary>
        public string Name => (string)Reference.Invoke("name");

        /// <summary>
        /// Returns the unique id of this query that persists across restarts from checkpoint data.
        /// That is, this id is generated when a query is started for the first time, and
        /// will be the same every time it is restarted from checkpoint data. Also see
        /// <see cref="RunId"/>.
        /// </summary>
        public string Id =>
            (string)((JvmObjectReference)Reference.Invoke("id")).Invoke("toString");

        /// <summary>
        /// Returns the unique id of this run of the query. That is, every start/restart of
        /// a query will generated a unique runId. Therefore, every time a query is restarted
        /// from checkpoint, it will have the same <see cref="Id"/> but different
        /// <see cref="RunId"/>s.
        /// </summary>
        public string RunId =>
            (string)((JvmObjectReference)Reference.Invoke("runId")).Invoke("toString");

        /// <summary>
        /// Returns true if this query is actively running.
        /// </summary>
        /// <returns>True if this query is actively running</returns>
        public bool IsActive() => (bool)Reference.Invoke("isActive");

        /// <summary>
        /// Waits for the termination of this query, either by Stop() or by an exception.
        /// </summary>
        public void AwaitTermination() => Reference.Invoke("awaitTermination");

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
            (bool)Reference.Invoke("awaitTermination", timeoutMs);

        /// <summary>
        /// Blocks until all available data in the source has been processed and committed to the
        /// sink. This method is intended for testing. Note that in the case of continually
        /// arriving data, this method may block forever. Additionally, this method is only
        /// guaranteed to block until data that has been synchronously appended data to a
        /// `org.apache.spark.sql.execution.streaming.Source` prior to invocation.
        /// (i.e. `getOffset` must immediately reflect the addition).
        /// </summary>
        public void ProcessAllAvailable() => Reference.Invoke("processAllAvailable");

        /// <summary>
        /// Stops the execution of this query if it is running. This method blocks until the
        /// threads performing execution stop.
        /// </summary>
        public void Stop() => Reference.Invoke("stop");

        /// <summary>
        /// Prints the physical plan to the console for debugging purposes.
        /// </summary>
        /// <param name="extended">Whether to do extended explain or not</param>
        public void Explain(bool extended = false) => Reference.Invoke("explain", extended);

        /// <summary>
        /// The <see cref="StreamingQueryException"/> if the query was terminated by an exception,
        /// null otherwise.
        /// </summary>
        public StreamingQueryException Exception()
        {
            var optionalException = new Option((JvmObjectReference)Reference.Invoke("exception"));
            if (optionalException.IsDefined())
            {
                var exception = (JvmObjectReference)optionalException.Get();
                return new StreamingQueryException((string)exception.Invoke("toString"));
            }

            return null;
        }
    }
}
