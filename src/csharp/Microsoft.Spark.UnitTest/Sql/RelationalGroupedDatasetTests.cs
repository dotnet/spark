// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Moq;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class RelationalGroupedDatasetTests
    {
        [Theory]
        [InlineData("2.4.0", false)]
        [InlineData("3.5.3", false)]
        [InlineData("4.0.0", true)]
        [InlineData("4.0.4", true)]
        public void GroupedMapUsesVersionSpecificJvmArgument(string version, bool usesColumn)
        {
            var jvm = new Mock<IJvmBridge>(MockBehavior.Strict);
            var groupReference = new JvmObjectReference("grouped", jvm.Object);
            var columnReference = new JvmObjectReference("column", jvm.Object);
            var expressionReference = new JvmObjectReference("expression", jvm.Object);
            var resultReference = new JvmObjectReference("result", jvm.Object);
            var column = new Column(columnReference);
            var grouped = new RelationalGroupedDataset(groupReference, null);
            object actualArgument = null;
            if (!usesColumn)
            {
                jvm.Setup(bridge => bridge.CallNonStaticJavaMethod(
                    columnReference, "expr", It.IsAny<object[]>())).Returns(expressionReference);
            }

            jvm.Setup(bridge => bridge.CallNonStaticJavaMethod(
                groupReference, "flatMapGroupsInPandas", It.IsAny<object>()))
                .Callback<JvmObjectReference, string, object>((_, __, argument) => actualArgument = argument)
                .Returns(resultReference);

            DataFrame result = grouped.ApplyGroupedMap(column, new Version(version));

            Assert.Same(resultReference, result.Reference);
            Assert.Same(usesColumn ? (object)column : expressionReference, actualArgument);
        }
    }
}
