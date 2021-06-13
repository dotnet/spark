// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.E2ETest.Utils
{
    /// <summary>
    /// A source of continually arriving data for a streaming query.
    /// Produces value stored in memory as they are added by the user.
    /// </summary>
    /// <typeparam name="T">
    /// Specifies the type of the elements contained in the MemoryStream.
    /// </typeparam>
    internal class MemoryStream<T> : IJvmObjectReferenceProvider
    {
        internal MemoryStream(SparkSession sparkSession)
        {
            JvmObjectReference sparkSessionRef = sparkSession.Reference;
            Reference = (JvmObjectReference)sparkSessionRef.Jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.test.TestUtils",
                "createMemoryStream",
                sparkSessionRef.Invoke("sqlContext"),
                typeof(T).Name);
        }

        public JvmObjectReference Reference { get; private set; }

        internal DataFrame ToDF() => new DataFrame((JvmObjectReference)Reference.Invoke("toDF"));

        // TODO: "addData" returns an Offset. Expose class if needed.	
        internal void AddData(T[] data) => Reference.Invoke("addData", data);
    }
}
