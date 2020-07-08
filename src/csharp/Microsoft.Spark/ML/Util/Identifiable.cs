// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark.ML.Feature
{
    public interface Identifiable
    {
        /// <summary>
        /// The UID of the object.
        /// </summary>
        /// <returns>string UID identifying the object</returns>
        string Uid();
    }
}
