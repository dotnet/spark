// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class WindowApiCompatibilityTests
    {
        [Fact]
        public void Spark2OnlyWindowMethodsAreRemovedAndLongBoundariesRemain()
        {
            foreach (Type type in new[] { typeof(Window), typeof(WindowSpec) })
            {
                Assert.Null(type.GetMethod("RangeBetween", new[] { typeof(Column), typeof(Column) }));
                Assert.NotNull(type.GetMethod("RangeBetween", new[] { typeof(long), typeof(long) }));
            }

            foreach (string name in new[] { "UnboundedPreceding", "UnboundedFollowing", "CurrentRow" })
            {
                Assert.Null(typeof(Functions).GetMethod(name, Type.EmptyTypes));
                Assert.Equal(typeof(long), typeof(Window).GetProperty(name).PropertyType);
            }
        }

        [Theory]
        [InlineData("FromUtcTimestamp")]
        [InlineData("ToUtcTimestamp")]
        public void DeprecatedTimestampMethodsRemainAvailable(string name)
        {
            Assert.NotNull(typeof(Functions).GetMethod(name, new[] { typeof(Column), typeof(string) }));
            Assert.NotNull(typeof(Functions).GetMethod(name, new[] { typeof(Column), typeof(Column) }));
        }
    }
}
