// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class DataFrameTests
    {
        [Theory]
        [InlineData("3.0.0", "collectToPython")]
        [InlineData("3.0.0", "toPythonIterator")]
        [InlineData("3.0.0", "tailToPython")]
        [InlineData("3.5.1", "collectToPython")]
        [InlineData("3.5.1", "toPythonIterator")]
        [InlineData("4.0.4", "collectToPython")]
        [InlineData("4.0.0", "tailToPython")]
        [InlineData("4.0.1", "tailToPython")]
        [InlineData("4.0.2", "tailToPython")]
        [InlineData("4.0.3", "tailToPython")]
        [InlineData("4.0.4", "tailToPython")]
        public void ValidateRowCollectionOperationAllowsSupportedOperations(
            string version,
            string funcName)
        {
            DataFrame.ValidateRowCollectionOperation(new Version(version), funcName);
        }

        [Theory]
        [InlineData("2.0.0", "collectToPython")]
        [InlineData("2.3.0", "collectToPython")]
        [InlineData("2.4.0", "collectToPython")]
        [InlineData("2.4.8", "toPythonIterator")]
        [InlineData("4.0.4", "toPythonIterator")]
        [InlineData("4.1.0", "collectToPython")]
        [InlineData("4.1.0", "tailToPython")]
        public void ValidateRowCollectionOperationRejectsUnsupportedOperations(
            string version,
            string funcName)
        {
            Assert.Throws<NotSupportedException>(() =>
                DataFrame.ValidateRowCollectionOperation(new Version(version), funcName));
        }
    }
}
