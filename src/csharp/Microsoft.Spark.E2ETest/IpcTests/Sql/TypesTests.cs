// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections;
using System.Collections.Generic;
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

        public class SimpleTypesGenerator : IEnumerable<object[]>
        {
            private readonly List<object[]> _data = new List<object[]>
            {
                new object[] { new NullType() },
                new object[] { new StringType() },
                new object[] { new BinaryType() },
                new object[] { new BooleanType() },
                new object[] { new DateType() },
                new object[] { new TimestampType() },
                new object[] { new DoubleType() },
                new object[] { new FloatType() },
                new object[] { new ByteType() },
                new object[] { new IntegerType() },
                new object[] { new LongType() },
                new object[] { new ShortType() },
                new object[] { new DecimalType() }
            };

            public IEnumerator<object[]> GetEnumerator() => _data.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        [Theory]
        [ClassData(typeof(SimpleTypesGenerator))]
        public void TestSimpleTypes(DataType simpleTypes)
        {
            Assert.IsType<JvmObjectReference>(DataType.FromJson(Jvm, simpleTypes.Json));
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
