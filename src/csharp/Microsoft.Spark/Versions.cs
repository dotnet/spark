// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Spark
{
    internal static class Versions
    {
        internal const string V2_3_0 = "2.3.0";
        internal const string V2_3_1 = "2.3.1";
        internal const string V2_3_2 = "2.3.2";
        internal const string V2_3_3 = "2.3.3";
        internal const string V2_4_0 = "2.4.0";
        internal const string V2_4_2 = "2.4.2";
        internal const string V3_0_0 = "3.0.0";

        // The following is used to check the compatibility of UDFs between
        // the driver and worker side. This needs to be updated only when there
        // is a breaking change on the UDF contract.
        internal const string CurrentVersion = "0.9.0";
    }
}
