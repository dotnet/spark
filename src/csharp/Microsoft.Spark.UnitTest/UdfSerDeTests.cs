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
                    return s;
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
                // Without Closure.
                Func<int, int> udf = i => 10 * i;
                VerifyUdfSerDe(udf, false);
            }

            {
                // With Closure where the Delegate target is an anonymous class.
                // The target will contain fields ["tc1", "tc2"], where "tc1" is
                // is non null and "tc2" is null.
                TestClass tc1 = new TestClass("Test");
                TestClass tc2 = null;
                Func<string, string> udf =
                    (s) =>
                    {
                        if (tc2 == null)
                        {
                            return tc1.Concat(s);
                        }
                        return string.Empty;
                    };
                VerifyUdfSerDe(udf, true);
            }

            {
                // With Closure where the Delegate target is TestClass
                // and target's field "_str" is set to "Test".
                TestClass tc = new TestClass("Test");
                Func<string, string> udf = tc.Concat;
                VerifyUdfSerDe(udf, true);
            }

            {
                // With Closure where the Delegate target is TestClass,
                // and target's field "_str" is set to null.
                TestClass tc = new TestClass(null);
                Func<string, string> udf = tc.Concat;
                VerifyUdfSerDe(udf, true);
            }
        }

        private void VerifyUdfSerDe(Delegate udf, bool hasClosure)
        {
            byte[] serializedUdf = SerializeUdf(udf);
            DeserializeAndVerify(udf, serializedUdf, hasClosure);
        }

        private byte[] SerializeUdf(Delegate udf)
        {
            UdfSerDe.UdfData udfData = UdfSerDe.Serialize(udf);

            using (var ms = new MemoryStream())
            {
                var bf = new BinaryFormatter();
                bf.Serialize(ms, udfData);
                return ms.ToArray();
            }
        }

        private void DeserializeAndVerify(
            Delegate udf,
            byte[] serializedUdf,
            bool hasClosure)
        {
            using (var ms = new MemoryStream(serializedUdf, false))
            {
                var bf = new BinaryFormatter();
                var udfData = (UdfSerDe.UdfData)bf.Deserialize(ms);
                VerifyUdfData(UdfSerDe.Serialize(udf), udfData, hasClosure);

                Delegate deserializedUdf = UdfSerDe.Deserialize(udfData);
                Assert.Equal(udf.GetType(), deserializedUdf.GetType());
                Assert.Equal(udf.Method, deserializedUdf.Method);
                Assert.Equal(udf.Target.GetType(), deserializedUdf.Target.GetType());

                FieldInfo[] expectedFields = udf.Target.GetType().GetFields();
                FieldInfo[] actualFields = deserializedUdf.Target.GetType().GetFields();
                Assert.Equal(expectedFields, actualFields);
            }
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
    }
}
