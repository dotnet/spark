// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;
using Moq;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class ColumnTestsFixture : IDisposable
    {
        internal Mock<IJvmBridge> MockJvm { get; }

        public ColumnTestsFixture()
        {
            MockJvm = new Mock<IJvmBridge>();

            MockJvm
                .Setup(m => m.CallStaticJavaMethod(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<object>()))
                .Returns(
                    new JvmObjectReference("result", MockJvm.Object));
            MockJvm
                .Setup(m => m.CallStaticJavaMethod(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<object>(),
                    It.IsAny<object>()))
                .Returns(
                    new JvmObjectReference("result", MockJvm.Object));
            MockJvm
                .Setup(m => m.CallStaticJavaMethod(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<object[]>()))
                .Returns(
                    new JvmObjectReference("result", MockJvm.Object));

            MockJvm
                .Setup(m => m.CallNonStaticJavaMethod(
                    It.IsAny<JvmObjectReference>(),
                    It.IsAny<string>(),
                    It.IsAny<object>()))
                .Returns(
                    new JvmObjectReference("result", MockJvm.Object));
            MockJvm
                .Setup(m => m.CallNonStaticJavaMethod(
                    It.IsAny<JvmObjectReference>(),
                    It.IsAny<string>(),
                    It.IsAny<object>(),
                    It.IsAny<object>()))
                .Returns(
                    new JvmObjectReference("result", MockJvm.Object));
            MockJvm
                .Setup(m => m.CallNonStaticJavaMethod(
                    It.IsAny<JvmObjectReference>(),
                    It.IsAny<string>(),
                    It.IsAny<object[]>()))
                .Returns(
                    new JvmObjectReference("result", MockJvm.Object));
        }

        public void Dispose()
        {
        }
    }

    public class ColumnTests : IClassFixture<ColumnTestsFixture>
    {
        private readonly Mock<IJvmBridge> _mockJvm;

        public ColumnTests(ColumnTestsFixture fixture)
        {
            _mockJvm = fixture.MockJvm;
        }

        private static JvmObjectId GetId(IJvmObjectReferenceProvider provider) => provider.Reference.Id;

        [Fact]
        public void TestColumnNegateOperator()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = -column1;

            _mockJvm.Verify(m => m.CallStaticJavaMethod(
                "org.apache.spark.sql.functions",
                "negate",
                column1), Times.Once);

            Assert.Equal("result", GetId(column2));
        }

        [Fact]
        public void TestNotOperator()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = !column1;

            _mockJvm.Verify(m => m.CallStaticJavaMethod(
                "org.apache.spark.sql.functions",
                "not",
                column1), Times.Once);

            Assert.Equal("result", GetId(column2));
        }

        [Fact]
        public void TestEqualOperator()
        {
            {
                // Column as a right operand.
                Column column1 = CreateColumn("col1");
                Column column2 = CreateColumn("col2");
                Column result = column1 == column2;
                VerifyNonStaticCall(column1, "equalTo", column2);
                Assert.Equal("result", GetId(result));
            }
            {
                // String as a right operand.
                // Note that any object can be used in place of string.
                Column column1 = CreateColumn("col1");
                Column result = column1 == "abc";
                VerifyNonStaticCall(column1, "equalTo", "abc");
                Assert.Equal("result", GetId(result));
            }
        }

        [Fact]
        public void TestNotEqualOperator()
        {
            {
                // Column as a right operand.
                Column column1 = CreateColumn("col1");
                Column column2 = CreateColumn("col2");
                Column result = column1 != column2;
                VerifyNonStaticCall(column1, "notEqual", column2);
                Assert.Equal("result", GetId(result));
            }
            {
                // String as a right operand.
                // Note that any object can be used in place of string.
                Column column1 = CreateColumn("col1");
                Column result = column1 != "abc";
                VerifyNonStaticCall(column1, "notEqual", "abc");
                Assert.Equal("result", GetId(result));
            }
        }

        [Fact]
        public void TestGreaterThanOperator()
        {
            {
                // Column as a right operand.
                Column column1 = CreateColumn("col1");
                Column column2 = CreateColumn("col2");
                Column result = column1 > column2;
                VerifyNonStaticCall(column1, "gt", column2);
                Assert.Equal("result", GetId(result));
            }
            {
                // String as a right operand.
                // Note that any object can be used in place of string.
                Column column1 = CreateColumn("col1");
                Column result = column1 > "abc";
                VerifyNonStaticCall(column1, "gt", "abc");
                Assert.Equal("result", GetId(result));
            }
        }

        [Fact]
        public void TestLessThanOperator()
        {
            {
                // Column as a right operand.
                Column column1 = CreateColumn("col1");
                Column column2 = CreateColumn("col2");
                Column result = column1 < column2;
                VerifyNonStaticCall(column1, "lt", column2);
                Assert.Equal("result", GetId(result));
            }
            {
                // String as a right operand.
                // Note that any object can be used in place of string.
                Column column1 = CreateColumn("col1");
                Column result = column1 < "abc";
                VerifyNonStaticCall(column1, "lt", "abc");
                Assert.Equal("result", GetId(result));
            }
        }

        [Fact]
        public void TestLessThanEqualToOperator()
        {
            {
                // Column as a right operand.
                Column column1 = CreateColumn("col1");
                Column column2 = CreateColumn("col2");
                Column result = column1 <= column2;
                VerifyNonStaticCall(column1, "leq", column2);
                Assert.Equal("result", GetId(result));
            }
            {
                // String as a right operand.
                // Note that any object can be used in place of string.
                Column column1 = CreateColumn("col1");
                Column result = column1 <= "abc";
                VerifyNonStaticCall(column1, "leq", "abc");
                Assert.Equal("result", GetId(result));
            }
        }

        [Fact]
        public void TestGreaterThanEqualToOperator()
        {
            {
                // Column as a right operand.
                Column column1 = CreateColumn("col1");
                Column column2 = CreateColumn("col2");
                Column result = column1 >= column2;
                VerifyNonStaticCall(column1, "geq", column2);
                Assert.Equal("result", GetId(result));
            }
            {
                // String as a right operand.
                // Note that any object can be used in place of string.
                Column column1 = CreateColumn("col1");
                Column result = column1 >= "abc";
                VerifyNonStaticCall(column1, "geq", "abc");
                Assert.Equal("result", GetId(result));
            }
        }

        [Fact]
        public void TestAndOperator()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = CreateColumn("col2");
            Column result = column1 & column2;
            VerifyNonStaticCall(column1, "and", column2);
            Assert.Equal("result", GetId(result));
        }

        [Fact]
        public void TestOrOperator()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = CreateColumn("col2");
            Column result = column1 | column2;
            VerifyNonStaticCall(column1, "or", column2);
            Assert.Equal("result", GetId(result));
        }

        [Fact]
        public void TestPlusOperator()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = CreateColumn("col2");
            Column result = column1 + column2;
            VerifyNonStaticCall(column1, "plus", column2);
            Assert.Equal("result", GetId(result));
        }

        [Fact]
        public void TestMinusOperator()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = CreateColumn("col2");
            Column result = column1 - column2;
            VerifyNonStaticCall(column1, "minus", column2);
            Assert.Equal("result", GetId(result));
        }

        [Fact]
        public void TestMultiplyOperator()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = CreateColumn("col2");
            Column result = column1 * column2;
            VerifyNonStaticCall(column1, "multiply", column2);
            Assert.Equal("result", GetId(result));
        }

        [Fact]
        public void TestDivideOperator()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = CreateColumn("col2");
            Column result = column1 / column2;
            VerifyNonStaticCall(column1, "divide", column2);
            Assert.Equal("result", GetId(result));
        }

        [Fact]
        public void TestModOperator()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = CreateColumn("col2");
            Column result = column1 % column2;
            VerifyNonStaticCall(column1, "mod", column2);
        }

        [Fact]
        public void TestWhenCondition()
        {
            Column column1 = CreateColumn("col1");
            Column column2 = CreateColumn("col2");
            int value = 0;
            column1.When(column2, value);
            VerifyNonStaticCall(column1, "when", column2, value);
        }

        [Fact]
        public void TestBetweenCondition()
        {
            Column column1 = CreateColumn("col1");
            int val1 = 1;
            int val2 = 2;
            column1.Between(val1, val2);
            VerifyNonStaticCall(column1, "between", val1, val2);
        }

        [Fact]
        public void TestSubStr()
        {
            {
                Column column1 = CreateColumn("col1");
                int pos = 1;
                int len = 2;
                column1.SubStr(pos, len);
                VerifyNonStaticCall(column1, "substr", pos, len);
            }
            {
                Column column1 = CreateColumn("col1");
                Column pos = CreateColumn("col2");
                Column len = CreateColumn("col3");
                column1.SubStr(pos, len);
                VerifyNonStaticCall(column1, "substr", pos, len);
            }
        }

        [Fact]
        public void TestOver()
        {
            {
                Column column1 = CreateColumn("col1");
                var windowSpec =
                    new WindowSpec(new JvmObjectReference("windowSpec", _mockJvm.Object));
                column1.Over();
                VerifyNonStaticCall(column1, "over");
            }
            {
                Column column1 = CreateColumn("col1");
                var windowSpec =
                    new WindowSpec(new JvmObjectReference("windowSpec", _mockJvm.Object));
                column1.Over(windowSpec);
                VerifyNonStaticCall(column1, "over", windowSpec);
            }
        }

        [Fact]
        public void TestIsIn()
        {   
            {
                var expected = new List<string> {"vararg_1", "vararg_2"};
                Column column1 = CreateColumn("col1");                
                column1.IsIn("vararg_1", "vararg_2");          
                
                VerifyNonStaticCall(column1, "isin", expected);
            }
            {
                Column column1 = CreateColumn("col1");
                var expected = new List<int>(){0, 1, 99};
                column1.IsIn(0, 1, 99);            
                VerifyNonStaticCall(column1, "isin", expected);
            }
            {
                Column column1 = CreateColumn("col1");
                var expected = new List<long>(){0L, 1L, 99L};
                column1.IsIn(0L, 1L, 99L);            
                VerifyNonStaticCall(column1, "isin", expected);
            }
            {
                Column column1 = CreateColumn("col1");
                var expected = new List<bool>(){true, false};
                column1.IsIn(true, false);            
                VerifyNonStaticCall(column1, "isin", expected);
            }
            {
                Column column1 = CreateColumn("col1");
                short short1 = 1;
                short short2 = 2;
                short short3 = 99;

                var expected = new List<short>(){short1, short2, short3};
                column1.IsIn(short1, short2, short3);            
                VerifyNonStaticCall(column1, "isin", expected);
            }            
            {
                Column column1 = CreateColumn("col1");
                var expected = new List<float>(){0F, 1F, 99F};
                column1.IsIn(0F, 1F, 99F);            
                VerifyNonStaticCall(column1, "isin", expected);
            }
            {
                Column column1 = CreateColumn("col1");
                var expected = new List<double>(){0.0, 1.0, 99.99};
                column1.IsIn(0.0, 1.0, 99.99);            
                VerifyNonStaticCall(column1, "isin", expected);
            }
            {
                Column column1 = CreateColumn("col1");
                decimal decimal1 = 1;
                decimal decimal2 = 2;
                decimal decimal3 = 3;

                var expected = new List<decimal>(){decimal1, decimal2, decimal3};
                column1.IsIn(decimal1, decimal2, decimal3);            
                VerifyNonStaticCall(column1, "isin", expected);
            }
        }

        private void VerifyNonStaticCall(
            IJvmObjectReferenceProvider obj,
            string methodName,
            object arg0)
        {
            _mockJvm.Verify(m => m.CallNonStaticJavaMethod(
                obj.Reference,
                methodName,
                arg0));
        }

        private void VerifyNonStaticCall(
            IJvmObjectReferenceProvider obj,
            string methodName,
            object arg0,
            object arg1)
        {
            _mockJvm.Verify(m => m.CallNonStaticJavaMethod(
                obj.Reference,
                methodName,
                arg0, arg1));
        }

        private void VerifyNonStaticCall(
            IJvmObjectReferenceProvider obj,
            string methodName,
            params object[] args)
        {
            _mockJvm.Verify(m => m.CallNonStaticJavaMethod(
                obj.Reference,
                methodName,
                args));
        }

        [Theory]
        [InlineData("EqNullSafe", "eqNullSafe")]
        [InlineData("Or", "or")]
        [InlineData("And", "and")]
        [InlineData("Contains", "contains")]
        [InlineData("StartsWith", "startsWith")]
        [InlineData("EndsWith", "endsWith")]
        [InlineData("EqualTo", "equalTo")]
        [InlineData("NotEqual", "notEqual")]
        [InlineData("Gt", "gt")]
        [InlineData("Lt", "lt")]
        [InlineData("Leq", "leq")]
        [InlineData("Geq", "geq")]
        [InlineData("Otherwise", "otherwise")]
        [InlineData("Plus", "plus")]
        [InlineData("Minus", "minus")]
        [InlineData("Multiply", "multiply")]
        [InlineData("Divide", "divide")]
        [InlineData("Mod", "mod")]
        [InlineData("GetItem", "getItem")]
        [InlineData("BitwiseOR", "bitwiseOR")]
        [InlineData("BitwiseAND", "bitwiseAND")]
        [InlineData("BitwiseXOR", "bitwiseXOR")]
        public void TestNamedOperators(string funcName, string opName)
        {
            Column column1 = CreateColumn("col1");
            Column column2 = CreateColumn("col2");
            System.Reflection.MethodInfo func = column1.GetType().GetMethod(
                funcName,
                new Type[] { typeof(Column) });
            var result = func.Invoke(column1, new[] { column2 }) as Column;
            VerifyNonStaticCall(column1, opName, column2);
            Assert.Equal("result", GetId(result));
        }

        [Theory]
        [InlineData("Contains", "contains")]
        [InlineData("StartsWith", "startsWith")]
        [InlineData("EndsWith", "endsWith")]
        [InlineData("Alias", "alias")]
        [InlineData("As", "alias")]
        [InlineData("Name", "name")]
        [InlineData("Cast", "cast")]
        [InlineData("Otherwise", "otherwise")]
        [InlineData("Like", "like")]
        [InlineData("RLike", "rlike")]
        [InlineData("GetItem", "getItem")]
        [InlineData("GetField", "getField")]
        public void TestNamedOperatorsWithString(string funcName, string opName)
        {
            // These operators take string as the operand.
            Column column = CreateColumn("col");
            string literal = "hello";
            System.Reflection.MethodInfo func = column.GetType().GetMethod(
                funcName,
                new Type[] { typeof(string) });
            var result = func.Invoke(column, new[] { literal }) as Column;
            Assert.Equal("result", GetId(result));
            VerifyNonStaticCall(column, opName, literal);
        }

        [Theory]
        [InlineData("Asc", "asc")]
        [InlineData("AscNullsFirst", "asc_nulls_first")]
        [InlineData("AscNullsLast", "asc_nulls_last")]
        [InlineData("Desc", "desc")]
        [InlineData("DescNullsFirst", "desc_nulls_first")]
        [InlineData("DescNullsLast", "desc_nulls_last")]
        [InlineData("IsNaN", "isNaN")]
        [InlineData("IsNull", "isNull")]
        [InlineData("IsNotNull", "isNotNull")]
        public void TestUnaryOperators(string funcName, string opName)
        {
            Column column = CreateColumn("col");

            // Use an empty array of Type objects to get a method that takes no parameters.
            System.Reflection.MethodInfo func =
                column.GetType().GetMethod(funcName, Type.EmptyTypes);
            var result = func.Invoke(column, null) as Column;
            Assert.Equal("result", GetId(result));
            VerifyNonStaticCall(column, opName);
        }

        private Column CreateColumn(string id)
        {
            return new Column(new JvmObjectReference(id, _mockJvm.Object));
        }
    }
}
