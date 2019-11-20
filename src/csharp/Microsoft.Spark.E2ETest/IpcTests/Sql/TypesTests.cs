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

            JvmObjectReference jvmArrayType = DataType.FromJson(Jvm, arrayType.Json);
            Assert.IsType<JvmObjectReference>(jvmArrayType);
        }

        [Fact]
        public void TestMapType()
        {
            var mapType = new MapType(new IntegerType(), new StringType());

            JvmObjectReference jvmMapType = DataType.FromJson(Jvm, mapType.Json);
            Assert.IsType<JvmObjectReference>(jvmMapType);
        }

        [Fact]
        public void TestStructType()
        {
            var structType = new StructType(new[]
            {
                new StructField("age", new IntegerType()),
                new StructField("name", new StringType())
            });

            JvmObjectReference jvmStructType = DataType.FromJson(Jvm, structType.Json);
            Assert.IsType<JvmObjectReference>(jvmStructType);

            // The following structField test failed for DataType.FromJson.
            // Since there is no direct match for structField from JVM side.
            // It will match for structType and redirect to structField.
            // <code>
            // var structField = structType.Fields[0];
            // JvmObjectReference jvmStructField = DataType.FromJson(Jvm, structField.JsonValue.ToString());
            // Assert.IsType<JvmObjectReference>(jvmStructType);
            // </code>
        }
    }
}
