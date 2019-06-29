// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
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

            public TestClass(int i)
            {
                _i = i;
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

                return this._i == that._i;
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
                VerifyUdfSerDe(udf, false, false);
            }

            {
                // With Closure
                TestClass tc = new TestClass(20);
                Func<int, int> udf = i => tc.MultiplyBy(i);
                VerifyUdfSerDe(udf, true, false);
            }

            {
                // With Closure where the udf target's Equals method is overridden.
                // The default behavior checks whether the two objects represent the
                // same object reference. However due to SerDe, the deserialized
                // object will be a new instance, and therefore we override Equals
                // to check the field values for equality.
                TestClass tc = new TestClass(20);
                Func<int, int> udf = tc.MultiplyBy;
                VerifyUdfSerDe(udf, true, true);
            }
        }

        private void VerifyUdfSerDe(Delegate udf, bool hasClosure, bool checkTargetEquality)
        {
            var serializedUdf = SerializeAndVerify(udf, hasClosure);
            DeserializeAndVerify(udf, serializedUdf, hasClosure, checkTargetEquality);
        }

        private byte[] SerializeAndVerify(Delegate udf, bool hasClosure)
        {
            UdfSerDe.UdfData udfData = UdfSerDe.Serialize(udf);
            VerifyUdfData(udf, udfData, hasClosure);

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
            bool hasClosure,
            bool checkTargetEquality)
        {
            using (var ms = new MemoryStream(serializedUdf, false))
            {
                var bf = new BinaryFormatter();
                UdfSerDe.UdfData udfData = (UdfSerDe.UdfData)bf.Deserialize(ms);
                VerifyUdfData(udf, udfData, hasClosure);

                Delegate deserializedUdf = UdfSerDe.Deserialize(udfData);
                Assert.Equal(udf.GetType(), deserializedUdf.GetType());
                Assert.Equal(udf.Method, deserializedUdf.Method);
                Assert.Equal(udf.Target.GetType(), deserializedUdf.Target.GetType());

                if (checkTargetEquality)
                {
                    Assert.Equal(udf.Target, deserializedUdf.Target);
                }
            }
        }

        private void VerifyUdfData(Delegate udf, UdfSerDe.UdfData udfData, bool hasClosure)
        {
            VerifyTypeData(udf.Method.DeclaringType, udfData.TypeData);
            Assert.Equal(udf.Method.Name, udfData.MethodName);
            VerifyTargetData(udf.Target, udfData.TargetData, hasClosure);
        }

        private void VerifyTypeData(Type type, UdfSerDe.TypeData typeData)
        {
            Assert.Equal(type.FullName, typeData.Name);
            Assert.Equal(type.Assembly.FullName, typeData.AssemblyName);
            Assert.Equal(type.Assembly.ManifestModule.Name, typeData.ManifestModuleName);
        }

        private void VerifyTargetData(
            object target,
            UdfSerDe.TargetData targetData,
            bool hasClosure)
        {
            Type targetType = target.GetType();
            VerifyTypeData(targetType, targetData.TypeData);

            // Fields are not serialized if there is no closure.
            if (!hasClosure)
            {
                Assert.Null(targetData.Fields);
                return;
            }

            // Check Fields
            UdfSerDe.FieldData[] actualFields = targetData.Fields;
            FieldInfo[] expectedFields = targetType.GetFields(
                BindingFlags.Instance |
                BindingFlags.Static |
                BindingFlags.Public |
                BindingFlags.NonPublic);
            Assert.Equal(expectedFields.Length, actualFields.Length);

            var actualFieldsDict = actualFields.ToDictionary(f => f.Name);
            foreach (FieldInfo expectedField in expectedFields)
            {
                Assert.True(
                    actualFieldsDict.TryGetValue(
                        expectedField.Name,
                        out UdfSerDe.FieldData actualField));

                VerifyTypeData(expectedField.FieldType, actualField.TypeData);
                Assert.Equal(expectedField.Name, actualField.Name);

                // Check ValueData
                object expectedValue = expectedField.GetValue(target);
                UdfSerDe.ValueData valueData = actualField.ValueData;
                VerifyTypeData(expectedValue.GetType(), valueData.TypeData);
                Assert.Equal(expectedValue, valueData.Value);
            }
        }
    }
}
