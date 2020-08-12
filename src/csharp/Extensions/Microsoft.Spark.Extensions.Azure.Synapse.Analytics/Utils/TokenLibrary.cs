// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;

namespace Microsoft.Spark.Extensions.Azure.Synapse.Analytics.Utils
{
    public sealed class TokenLibrary
    {
        /// <summary>
        /// Get a connection string for a given linked service.
        /// </summary>
        /// <param name="linkedServiceName">Name of the linked service</param>
        /// <returns>Connection string</returns>
        public static string GetConnectionString(string linkedServiceName) =>
            (string)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "com.microsoft.azure.synapse.tokenlibrary.TokenLibrary",
                "getConnectionString",
                linkedServiceName);
    }
}
