// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Streaming
{
    /// <summary>
    /// Policy used to indicate how often results should be produced by a 
    /// <see cref="StreamingQuery"/>
    /// </summary>
    public sealed class Trigger : IJvmObjectReferenceProvider
    {
        private static IJvmBridge Jvm { get; } = SparkEnvironment.JvmBridge;
        private static readonly string s_triggerClassName = 
            "org.apache.spark.sql.streaming.Trigger";

        internal Trigger(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// A trigger policy that runs a query periodically based on an interval 
        /// in processing time.
        /// If `interval` is 0, the query will run as fast as possible.
        /// </summary>
        /// <param name="intervalMs">Milliseconds</param>
        /// <returns>Trigger Object</returns>
        public static Trigger ProcessingTime(long intervalMs)
        {
            return Apply("ProcessingTime", intervalMs);
        }

        /// <summary>
        /// A trigger policy that runs a query periodically based on an interval 
        /// in processing time.
        /// If `interval` is effectively 0, the query will run as fast as possible.
        /// </summary>
        /// <param name="interval">string representation for interval. eg. "10 seconds"</param>
        /// <returns>Trigger Object</returns>
        public static Trigger ProcessingTime(string interval)
        {
            return Apply("ProcessingTime", interval);
        }

        /// <summary>
        /// A trigger that process only one batch of data in a streaming query 
        /// then terminates the query.
        /// </summary>
        /// <returns>Trigger Object</returns>
        public static Trigger Once()
        {
            return Apply("Once");
        }

        /// <summary>
        /// A trigger that continuously processes streaming data,
        /// asynchronously checkpointing at the specified interval.
        /// </summary>
        /// <param name="intervalMs">Milliseconds</param>
        /// <returns>Trigger Object</returns>
        public static Trigger Continuous(long intervalMs)
        {
            return Apply("Continuous", intervalMs);
        }

        /// <summary>
        /// A trigger that continuously processes streaming data,
        /// asynchronously checkpointing at the specified interval.
        /// </summary>
        /// <param name="interval">string representation for interval. eg. "10 seconds"</param>
        /// <returns>Trigger Object</returns>
        public static Trigger Continuous(string interval)
        {
            return Apply("Continuous", interval);
        }

        private static Trigger Apply(string funcName, params object[] args)
        {
            return new Trigger(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_triggerClassName,
                    funcName,
                    args));
        }
    }
}
