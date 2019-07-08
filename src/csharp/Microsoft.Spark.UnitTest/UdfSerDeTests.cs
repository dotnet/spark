// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            private readonly int _i;
            public object _obj;

            public TestClass(int i)
            {
                _i = i;
                _obj = null;
            }

            public int MultiplyBy(int i)
            {
                return _i * i;
            }

            public override bool Equals(object obj)
            {
                var that = obj as TestClass;

                if (that == null)
                {
                    return false;
                }

                return (_i == that._i) &&
                    ((_obj == null && that._obj == null) ||
                    (_obj != null && _obj.Equals(that._obj)));
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
                // Without Closure
                Func<int, int> udf = i => 10 * i;
                VerifyUdfSerDe(udf, false);
            }

            {
                // With Closure where the Delegate target is an anonymous class.
                TestClass tc = new TestClass(20);
                Func<int, int> udf = i => tc.MultiplyBy(i);
                VerifyUdfSerDe(udf, true);
            }

            {
                // With Closure where the Delegate target is TestClass
                TestClass tc = new TestClass(20);
                Func<int, int> udf = tc.MultiplyBy;
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
                VerifyUdfData(udf, udfData, hasClosure);

                Delegate deserializedUdf = UdfSerDe.Deserialize(udfData);
                Assert.Equal(udf.GetType(), deserializedUdf.GetType());
                Assert.Equal(udf.Method, deserializedUdf.Method);
                Assert.Equal(udf.Target.GetType(), deserializedUdf.Target.GetType());

                FieldInfo[] expectedFields = udf.Target.GetType().GetFields();
                FieldInfo[] actualFields = deserializedUdf.Target.GetType().GetFields();
                Assert.Equal(expectedFields.Length, actualFields.Length);

                Dictionary<string, FieldInfo> actualFieldsDict =
                    actualFields.ToDictionary(f => f.Name);
                foreach (FieldInfo expectedField in expectedFields)
                {
                    Assert.True(
                        actualFieldsDict.TryGetValue(
                            expectedField.Name,
                            out FieldInfo actualField));

                    Assert.Equal(expectedField, actualField);
                }
            }
        }

        private void VerifyUdfData(
            Delegate expectedUdf,
            UdfSerDe.UdfData actualUdfData,
            bool hasClosure)
        {
            UdfSerDe.UdfData expectedUdfData = UdfSerDe.Serialize(expectedUdf);
            Assert.Equal(expectedUdfData, actualUdfData);

            if (!hasClosure)
            {
                Assert.Equal(expectedUdfData.TargetData.Fields, UdfSerDe.TargetData.s_emptyFields);
                Assert.Equal(actualUdfData.TargetData.Fields, UdfSerDe.TargetData.s_emptyFields);
            }
        }
    }
}
