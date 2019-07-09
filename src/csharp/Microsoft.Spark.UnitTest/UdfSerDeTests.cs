// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class UdfSerDeTests
    {
        [Serializable]
        private class TestClass
        {
            private string _str;

            public TestClass(string s)
            {
                _str = s;
            }

            public string Concat(string s)
            {
                if (_str == null)
                {
                    return s + s;
                }

                return _str + s;
            }

            public override bool Equals(object obj)
            {
                var that = obj as TestClass;

                if (that == null)
                {
                    return false;
                }

                return _str == that._str;
            }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }
        }

        [Fact]
        public void TestUdfSerDe()
        {
            {
                // Without closure.
                Func<int, int> expectedUdf = i => 10 * i;
                VerifyUdfSerDe(expectedUdf, false, out Delegate actualUdf);

                int input = 10;
                int expectedResult = 100;
                Assert.Equal(expectedResult, expectedUdf(input));
                Assert.Equal(expectedResult, ((Func<int, int>)actualUdf)(input));
            }

            {
                // With closure where the delegate target is an anonymous class.
                // The target will contain fields ["tc1", "tc2"], where "tc1" is
                // non null and "tc2" is null.
                TestClass tc1 = new TestClass("Test");
                TestClass tc2 = null;
                Func<string, string> expectedUdf =
                    (s) =>
                    {
                        if (tc2 == null)
                        {
                            return tc1.Concat(s);
                        }
                        return s;
                    };
                VerifyUdfSerDe(expectedUdf, true, out Delegate actualUdf);

                string input = "HelloWorld";
                string expectedResult = "TestHelloWorld";
                Assert.Equal(expectedResult, expectedUdf(input));
                Assert.Equal(expectedResult, ((Func<string, string>)actualUdf)(input));
            }

            {
                // With closure where the delegate target is TestClass
                // and target's field "_str" is set to "Test".
                TestClass tc = new TestClass("Test");
                Func<string, string> expectedUdf = tc.Concat;
                VerifyUdfSerDe(expectedUdf, true, out Delegate actualUdf);

                string input = "HelloWorld";
                string expectedResult = "TestHelloWorld";
                Assert.Equal(expectedResult, expectedUdf(input));
                Assert.Equal(expectedResult, ((Func<string, string>)actualUdf)(input));
            }

            {
                // With closure where the delegate target is TestClass,
                // and target's field "_str" is set to null.
                TestClass tc = new TestClass(null);
                Func<string, string> expectedUdf = tc.Concat;
                VerifyUdfSerDe(expectedUdf, true, out Delegate actualUdf);

                string input = "HelloWorld";
                string expectedResult = "HelloWorldHelloWorld";
                Assert.Equal(expectedResult, expectedUdf(input));
                Assert.Equal(expectedResult, ((Func<string, string>)actualUdf)(input));
            }
        }

        private void VerifyUdfSerDe(Delegate udf, bool hasClosure, out Delegate deserializedUdf)
        {
            byte[] serializedUdf = Serialize(udf, out UdfSerDe.UdfData serializedUdfData);
            deserializedUdf =
                Deserialize(serializedUdf, out UdfSerDe.UdfData deseraizliedUdfData);

            VerifyUdfData(serializedUdfData, deseraizliedUdfData, hasClosure);
            VerifyDeleagte(udf, deserializedUdf);
        }

        private void VerifyUdfData(
            UdfSerDe.UdfData expectedUdfData,
            UdfSerDe.UdfData actualUdfData,
            bool hasClosure)
        {
            Assert.Equal(expectedUdfData, actualUdfData);

            if (!hasClosure)
            {
                Assert.Null(expectedUdfData.TargetData.Fields);
                Assert.Null(actualUdfData.TargetData.Fields);
            }
        }

        private void VerifyDeleagte(Delegate expectedDelegate, Delegate actualDelegate)
        {
            Assert.Equal(expectedDelegate.GetType(), actualDelegate.GetType());
            Assert.Equal(expectedDelegate.Method, actualDelegate.Method);
            Assert.Equal(expectedDelegate.Target.GetType(), actualDelegate.Target.GetType());

            FieldInfo[] expectedFields = expectedDelegate.Target.GetType().GetFields();
            FieldInfo[] actualFields = actualDelegate.Target.GetType().GetFields();
            Assert.Equal(expectedFields, actualFields);
        }

        private byte[] Serialize(Delegate udf, out UdfSerDe.UdfData udfData)
        {
            udfData = UdfSerDe.Serialize(udf);

            using (var ms = new MemoryStream())
            {
                var bf = new BinaryFormatter();
                bf.Serialize(ms, udfData);
                return ms.ToArray();
            }
        }

        private Delegate Deserialize(byte[] serializedUdf, out UdfSerDe.UdfData udfData)
        {
            using (var ms = new MemoryStream(serializedUdf, false))
            {
                var bf = new BinaryFormatter();
                udfData = (UdfSerDe.UdfData)bf.Deserialize(ms);
                return UdfSerDe.Deserialize(udfData);
            }
        }
    }
}
