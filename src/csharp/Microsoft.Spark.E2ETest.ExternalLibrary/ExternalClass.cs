// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.E2ETest.ExternalLibrary
{
    [Serializable]
    public class ExternalClass
    {
        private string s;

        public ExternalClass(string s)
        {
            this.s = s;
        }

        public static string HelloWorld()
        {
            return "Hello World";
        }

        public string Concat(string s)
        {
            return this.s + s;
        }
    }
}
