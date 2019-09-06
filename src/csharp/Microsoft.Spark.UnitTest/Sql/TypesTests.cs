// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql.Types;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class TypesTests
    {
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
            var atomicType = DataType.ParseDataType($@"""{typeName}""");
            Assert.Equal(typeName, atomicType.TypeName);
            Assert.Equal(typeName, atomicType.SimpleString);
        }

        [Fact]
        public void TestArrayType()
        {
            string schemaJson =
                @"{
                    ""type"":""array"",
                    ""elementType"":""integer"",
                    ""containsNull"":false
                }";
            var arrayType = (ArrayType)DataType.ParseDataType(schemaJson);
            Assert.Equal("array", arrayType.TypeName);
            Assert.Equal("array<integer>", arrayType.SimpleString);
            Assert.Equal("integer", arrayType.ElementType.TypeName);
            Assert.False(arrayType.ContainsNull);
        }
        
        [Fact]
        public void TestMapType()
        {
            string schemaJson =
                @"{
                    ""type"":""map"",
                    ""keyType"":""integer"",
                    ""valueType"":""double"",
                    ""valueContainsNull"":false
                }";
            var mapType = (MapType)DataType.ParseDataType(schemaJson);
            Assert.Equal("map", mapType.TypeName);
            Assert.Equal("map<integer,double>", mapType.SimpleString);
            Assert.Equal("integer", mapType.KeyType.TypeName);
            Assert.Equal("double", mapType.ValueType.TypeName);
            Assert.False(mapType.ValueContainsNull);
        }
        
        [Fact]
        public void TestStructTypeAndStructFieldTypes()
        {
            string schemaJson =
                @"{
                    ""type"":""struct"",
                    ""fields"":[
                        {
                            ""name"":""age"",
                            ""type"":""long"",
                            ""nullable"":true,
                            ""metadata"":{}
                        },
                        {
                            ""name"":""name"",
                            ""type"":""string"",
                            ""nullable"":false,
                            ""metadata"":{}
                        }
                    ]}";

            var structType = (StructType)DataType.ParseDataType(schemaJson);
            Assert.Equal("struct", structType.TypeName);
            Assert.Equal("struct<age:long,name:string>", structType.SimpleString);
            Assert.Equal(2, structType.Fields.Count);

            {
                StructField field = structType.Fields[0];
                Assert.Equal("age", field.Name);
                Assert.Equal("long", field.DataType.TypeName);
                Assert.True(field.IsNullable);
                Assert.Equal(new JObject(), field.Metadata);
            }
            {
                StructField field = structType.Fields[1];
                Assert.Equal("name", field.Name);
                Assert.Equal("string", field.DataType.TypeName);
                Assert.False(field.IsNullable);
                Assert.Equal(new JObject(), field.Metadata);
            }
        }
    }
}
