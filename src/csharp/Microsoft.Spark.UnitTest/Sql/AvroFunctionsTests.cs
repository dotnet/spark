// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql.Avro;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class AvroFunctionsTests
    {
        [Theory]
        [InlineData("3.0.0")]
        [InlineData("3.1.1")]
        [InlineData("3.2.0")]
        [InlineData("3.3.0")]
        [InlineData("3.4.0")]
        [InlineData("3.5.3")]
        [InlineData("4.0.0")]
        [InlineData("4.0.1")]
        [InlineData("4.0.2")]
        [InlineData("4.0.3")]
        [InlineData("4.0.4")]
        public void SupportedVersionsUseExistingAvroFunctions(string version)
        {
            Assert.Equal("org.apache.spark.sql.avro.functions",
                Functions.GetAvroClassName(new Version(version)));
        }

        [Theory]
        [InlineData("2.0.0")]
        [InlineData("2.4.8")]
        [InlineData("4.1.0")]
        [InlineData("5.0.0")]
        public void UnsupportedVersionsAreRejected(string version)
        {
            NotSupportedException error = Assert.Throws<NotSupportedException>(() =>
                Functions.GetAvroClassName(new Version(version)));
            Assert.Contains(version, error.Message);
        }
    }
}
