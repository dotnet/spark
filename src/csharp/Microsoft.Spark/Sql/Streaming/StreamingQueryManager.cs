// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Streaming
{
    /// <summary>
    /// A class to manage all the <see cref="StreamingQuery"/> active
    /// in a <see cref="SparkSession"/>.
    /// </summary>
    public sealed class StreamingQueryManager : IJvmObjectReferenceProvider
    {
        internal StreamingQueryManager(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Returns a list of active queries associated with this SQLContext.
        /// </summary>
        /// <returns>Active queries associated with this SQLContext.</returns>
        public IEnumerable<StreamingQuery> Active() =>
            ((JvmObjectReference[])Reference.Invoke("active"))
                .Select(sq => new StreamingQuery(sq));

        /// <summary>
        /// Returns an active query from this SQLContext or throws exception if an active
        /// query with this name doesn't exist.
        /// </summary>
        /// <param name="id">Id of the <see cref="StreamingQuery"/>.</param>
        /// <returns>
        /// <see cref="StreamingQuery"/> if there is an active query with the given id.
        /// </returns>
        public StreamingQuery Get(string id) =>
            new StreamingQuery((JvmObjectReference)Reference.Invoke("get", id));

        /// <summary>
        /// Wait until any of the queries on the associated SQLContext has terminated since the
        /// creation of the context, or since <see cref="ResetTerminated"/> was called. If any
        /// query was terminated with an exception, then the exception will be thrown.
        ///
        /// If a query has terminated, then subsequent calls to <see cref="AwaitAnyTermination()"/>
        /// will either return immediately (if the query was terminated by
        /// <see cref="StreamingQuery.Stop"/>), or throw the exception immediately (if the query
        /// was terminated with exception). Use <see cref="ResetTerminated"/> to clear past
        /// terminations and wait for new terminations.
        ///
        /// In the case where multiple queries have terminated since <see cref="ResetTerminated"/>
        /// was called, if any query has terminated with exception, then
        /// <see cref="AwaitAnyTermination()"/> will throw any of the exception. For correctly
        /// documenting exceptions across multiple queries, users need to stop all of them after
        /// any of them terminates with exception, and then check the
        /// <see cref="StreamingQuery.Exception"/> for each query.
        ///
        /// Throws StreamingQueryException on the JVM if any query has terminated with an
        /// exception.
        /// </summary>
        public void AwaitAnyTermination() => Reference.Invoke("awaitAnyTermination");

        /// <summary>
        /// Wait until any of the queries on the associated SQLContext has terminated since the
        /// creation of the context, or since <see cref="ResetTerminated"/> was called. If any
        /// query was terminated with an exception, then the exception will be thrown.
        ///
        /// If a query has terminated, then subsequent calls to <see cref="AwaitAnyTermination()"/>
        /// will either return immediately (if the query was terminated by
        /// <see cref="StreamingQuery.Stop"/>), or throw the exception immediately (if the query
        /// was terminated with exception). Use <see cref="ResetTerminated"/> to clear past
        /// terminations and wait for new terminations.
        ///
        /// In the case where multiple queries have terminated since <see cref="ResetTerminated"/>
        /// was called, if any query has terminated with exception, then
        /// <see cref="AwaitAnyTermination()"/> will throw any of the exception. For correctly
        /// documenting exceptions across multiple queries, users need to stop all of them after
        /// any of them terminates with exception, and then check the
        /// <see cref="StreamingQuery.Exception"/> for each query.
        ///
        /// Throws StreamingQueryException on the JVM if any query has terminated with an
        /// exception.
        /// </summary>
        /// <param name="timeoutMs">
        /// Milliseconds to wait for query to terminate. Returns whether the query has terminated
        /// or not.
        /// </param>
        public void AwaitAnyTermination(long timeoutMs) =>
            Reference.Invoke("awaitAnyTermination", timeoutMs);

        /// <summary>
        /// Forget about past terminated queries so that <see cref="AwaitAnyTermination()"/> can be
        /// used again to wait for new terminations
        /// </summary>
        public void ResetTerminated() => Reference.Invoke("resetTerminated");
    }
}
