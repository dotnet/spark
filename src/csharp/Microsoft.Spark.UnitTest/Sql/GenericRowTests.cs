// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class GenericRowTests
    {        
        [Fact]
        public void GenericRowTest()
        {            
            var row = new GenericRow(new object[] { 1, "abc" });

            // Validate Size().
            Assert.Equal(2, row.Size());

            // Validate [] operator.
            Assert.Equal(1, row[0]);
            Assert.Equal("abc", row[1]);

            // Validate Get*(int).
            Assert.Equal(1, row.Get(0));
            Assert.Equal("abc", row.Get(1));
            Assert.Equal(1, row.GetAs<int>(0));
            Assert.ThrowsAny<Exception>(() => row.GetAs<string>(0));
            Assert.Equal("abc", row.GetAs<string>(1));
            Assert.ThrowsAny<Exception>(() => row.GetAs<int>(1));            
        }        
    }
}
