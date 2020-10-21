// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;
using Xunit;

namespace Microsoft.Spark.UnitTest.TestUtils
{
    public static class ArrowTestUtils
    {
        public static void AssertEquals(string expectedValue, IArrowArray arrowArray)
        {
            Assert.IsType<StringArray>(arrowArray);
            var stringArray = (StringArray)arrowArray;
            Assert.Equal(1, stringArray.Length);
            Assert.Equal(expectedValue, stringArray.GetString(0));
        }

        public static void AssertEquals(string expectedValue, DataFrameColumn arrowArray)
        {
            var stringArray = (ArrowStringDataFrameColumn)arrowArray;
            Assert.Equal(1, stringArray.Length);
            Assert.Equal(expectedValue, stringArray[0]);
        }

        public static IArrowType GetArrowType<T>()
        {
            Type type = typeof(T);
            return type switch
            {
                _ when type == typeof(bool) => BooleanType.Default,
                _ when type == typeof(sbyte) => Int8Type.Default,
                _ when type == typeof(byte) => UInt8Type.Default,
                _ when type == typeof(short) => Int16Type.Default,
                _ when type == typeof(ushort) => UInt16Type.Default,
                _ when type == typeof(int) => Int32Type.Default,
                _ when type == typeof(uint) => UInt32Type.Default,
                _ when type == typeof(long) => Int64Type.Default,
                _ when type == typeof(ulong) => UInt64Type.Default,
                _ when type == typeof(float) => FloatType.Default,
                _ when type == typeof(double) => DoubleType.Default,
                _ when type == typeof(DateTime) => Date64Type.Default,
                _ when type == typeof(TimeSpan) => TimestampType.Default,
                _ when type == typeof(string) => StringType.Default,
                _ when type == typeof(byte[]) => BinaryType.Default,
                _ => throw new NotSupportedException($"Unknown type: {typeof(T)}")
            };
        }

        public static ArrowStringDataFrameColumn ToArrowStringDataFrameColumn(StringArray array)
        {
            return new ArrowStringDataFrameColumn("String",
                array.ValueBuffer.Memory,
                array.ValueOffsetsBuffer.Memory,
                array.NullBitmapBuffer.Memory,
                array.Length,
                array.NullCount);
        }

        public static IArrowArray ToArrowArray<T>(T[] array)
        {
            Type type = typeof(T);
            object arrayObject = array;
            return type switch
            {
                _ when type == typeof(bool) => ToBooleanArray((bool[])arrayObject),
                _ when type == typeof(sbyte) => ToPrimitiveArrowArray((sbyte[])arrayObject),
                _ when type == typeof(byte) => ToPrimitiveArrowArray((byte[])arrayObject),
                _ when type == typeof(short) => ToPrimitiveArrowArray((short[])arrayObject),
                _ when type == typeof(ushort) => ToPrimitiveArrowArray((ushort[])arrayObject),
                _ when type == typeof(int) => ToPrimitiveArrowArray((int[])arrayObject),
                _ when type == typeof(uint) => ToPrimitiveArrowArray((uint[])arrayObject),
                _ when type == typeof(long) => ToPrimitiveArrowArray((long[])arrayObject),
                _ when type == typeof(ulong) => ToPrimitiveArrowArray((ulong[])arrayObject),
                _ when type == typeof(float) => ToPrimitiveArrowArray((float[])arrayObject),
                _ when type == typeof(double) => ToPrimitiveArrowArray((double[])arrayObject),
                _ when type == typeof(DateTime) => ToPrimitiveArrowArray((DateTime[])arrayObject),
                _ when type == typeof(TimeSpan) => ToPrimitiveArrowArray((TimeSpan[])arrayObject),
                _ when type == typeof(string) => ToStringArrowArray((string[])arrayObject),
                _ when type == typeof(byte[]) => ToBinaryArrowArray((byte[][])arrayObject),
                _ => throw new NotSupportedException($"Unknown type: {typeof(T)}")
            };
        }

        public static IArrowArray ToPrimitiveArrowArray<T>(T[] array) where T : struct
        {
            var builder = new ArrowBuffer.Builder<T>(array.Length);

            // TODO: The builder should have an API for blitting an array, or its IEnumerable
            // AppendRange should special-case T[] to do that directly when possible.
            foreach (T item in array)
            {
                builder.Append(item);
            }

            var data = new ArrayData(
                GetArrowType<T>(),
                array.Length,
                0,
                0,
                new[] { ArrowBuffer.Empty, builder.Build() });

            return ArrowArrayFactory.BuildArray(data);
        }

        private static IArrowArray ToBooleanArray(bool[] array)
        {
            byte[] rawBytes = CreateRawBytesForBoolArray(array.Length);
            for (int i = 0; i < array.Length; ++i)
            {
                // only need to set true values since rawBytes is zeroed
                // by the .NET runtime.
                if (array[i])
                {
                    BitUtility.SetBit(rawBytes, i);
                }
            }

            var builder = new ArrowBuffer.Builder<byte>(rawBytes.Length);
            builder.AppendRange(rawBytes);

            var data = new ArrayData(
                BooleanType.Default,
                array.Length,
                0,
                0,
                new[] { ArrowBuffer.Empty, builder.Build() });

            return ArrowArrayFactory.BuildArray(data);
        }

        private static byte[] CreateRawBytesForBoolArray(int boolArrayLength)
        {
            int byteLength = boolArrayLength / 8;
            if (boolArrayLength % 8 != 0)
            {
                ++byteLength;
            }

            return new byte[byteLength];
        }

        private static IArrowArray ToStringArrowArray(string[] array)
        {
            var valueOffsets = new ArrowBuffer.Builder<int>();
            var valueBuffer = new ArrowBuffer.Builder<byte>();
            int offset = 0;

            // TODO: Use array pool and encode directly into the array.
            foreach (string str in array)
            {
                byte[] bytes = Encoding.UTF8.GetBytes(str);
                valueOffsets.Append(offset);
                // TODO: Anyway to use the span-based GetBytes to write directly to
                // the value buffer?
                valueBuffer.Append(bytes);
                offset += bytes.Length;
            }

            valueOffsets.Append(offset);
            return new StringArray(
                new ArrayData(
                    StringType.Default,
                    valueOffsets.Length - 1,
                    0,
                    0,
                    new[] { ArrowBuffer.Empty, valueOffsets.Build(), valueBuffer.Build() }));
        }

        private static IArrowArray ToBinaryArrowArray(byte[][] array)
        {
            var valueOffsets = new ArrowBuffer.Builder<int>();
            var valueBuffer = new ArrowBuffer.Builder<byte>();
            int offset = 0;

            foreach (byte[] bytes in array)
            {
                valueOffsets.Append(offset);
                // TODO: Anyway to use the span-based GetBytes to write directly to
                // the value buffer?
                valueBuffer.Append(bytes);
                offset += bytes.Length;
            }

            valueOffsets.Append(offset);
            return new StringArray(
                new ArrayData(
                    BinaryType.Default,
                    valueOffsets.Length - 1,
                    0,
                    0,
                    new[] { ArrowBuffer.Empty, valueOffsets.Build(), valueBuffer.Build() }));
        }
    }
}
