// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class TypesTests
    {
        private static IJvmBridge Jvm { get; } = SparkEnvironment.JvmBridge;

        [Theory]
        [InlineData("null")]
        [InlineData("string")]
        [InlineData("binary")]
        [InlineData("boolean")]
        [InlineData("date")]
        [InlineData("timestamp")]
        [InlineData("double")]
        [InlineData("float")]
        [InlineData("byte")]
        [InlineData("integer")]
        [InlineData("long")]
        [InlineData("short")]
        public void TestSimpleTypes(string typeName)
        {
            JvmObjectReference atomicType = DataType.FromJson(Jvm, $@"""{typeName}""");
            Assert.IsType<JvmObjectReference>(atomicType);
        }

        [Fact]
        public void TestArrayType()
        {
            var arrayType = new ArrayType(new IntegerType());
            Assert.IsType<JvmObjectReference>(DataType.FromJson(Jvm, arrayType.Json));
        }

        [Fact]
        public void TestMapType()
        {
            var mapType = new MapType(new IntegerType(), new StringType());
            Assert.IsType<JvmObjectReference>(DataType.FromJson(Jvm, mapType.Json));
        }

        [Fact]
        public void TestStructType()
        {
            // Please note that StructField is not supported.
            var structType = new StructType(new[]
            {
                new StructField("age", new IntegerType()),
                new StructField("name", new StringType())
            });
            Assert.IsType<JvmObjectReference>(DataType.FromJson(Jvm, structType.Json));
        }
    }
}
