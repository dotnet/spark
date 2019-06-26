// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Streaming
{
    public sealed class Trigger : IJvmObjectReferenceProvider
    {
        private static IJvmBridge Jvm { get; } = SparkEnvironment.JvmBridge;
        private static readonly string s_functionsClassName = "org.apache.spark.sql.streaming.Trigger";


        private readonly JvmObjectReference _jvmObject;
        internal Trigger(JvmObjectReference jvmObject) => _jvmObject = jvmObject;
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// A trigger policy that runs a query periodically based on an interval in processing time.
        /// If `interval` is 0, the query will run as fast as possible.
        /// </summary>
        /// <param name="intervalMs">milliseconds</param>
        /// <returns>Trigger Object</returns>
        public static Trigger ProcessingTime(long intervalMs)
        {
            return ApplyFunction("ProcessingTime", intervalMs);
        }

        /// <summary>
        /// A trigger that continuously processes streaming data, asynchronously checkpointing at the specified interval.
        /// </summary>
        /// <returns>Trigger Object</returns>
        public static Trigger Once()
        {
            return ApplyFunction("Once");
        }

        /// <summary>
        /// A trigger policy that runs a query periodically based on an interval in processing time.
        /// If `interval` is 0, the query will run as fast as possible.
        /// </summary>
        /// <param name="intervalMs">milliseconds</param>
        /// <returns>Trigger Object</returns>
        public static Trigger Continuous(long intervalMs)
        {
            return ApplyFunction("Continuous", intervalMs);
        }


        private static Trigger ApplyFunction(string funcName, params object[] args)
        {
            return new Trigger(
                (JvmObjectReference)Jvm.CallStaticJavaMethod(
                    s_functionsClassName,
                    funcName,
                    args));
        }
    }
}
