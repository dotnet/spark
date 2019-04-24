// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Apache.Arrow;
using Xunit;

namespace Microsoft.Spark.UnitTest.TestUtils
{
    public static class ArrowTestUtils
    {
        public static void AssertEquals(string expectedValue, IArrowArray arrowArray)
        {
            Assert.IsType<StringArray>(arrowArray);
            var stringArray = (StringArray)arrowArray;
            Assert.Equal(1, stringArray.Length);
            Assert.Equal(expectedValue, stringArray.GetString(0));
        }
    }
}
