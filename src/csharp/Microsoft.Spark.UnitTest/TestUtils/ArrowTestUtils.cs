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
            if (typeof(T) == typeof(bool))
            {
                return BooleanType.Default;
            }

            if (typeof(T) == typeof(sbyte))
            {
                return Int8Type.Default;
            }

            if (typeof(T) == typeof(byte))
            {
                return UInt8Type.Default;
            }

            if (typeof(T) == typeof(short))
            {
                return Int16Type.Default;
            }

            if (typeof(T) == typeof(ushort))
            {
                return UInt16Type.Default;
            }

            if (typeof(T) == typeof(int))
            {
                return Int32Type.Default;
            }

            if (typeof(T) == typeof(uint))
            {
                return UInt32Type.Default;
            }

            if (typeof(T) == typeof(long))
            {
                return Int64Type.Default;
            }

            if (typeof(T) == typeof(ulong))
            {
                return UInt64Type.Default;
            }

            if (typeof(T) == typeof(float))
            {
                return FloatType.Default;
            }

            if (typeof(T) == typeof(double))
            {
                return DoubleType.Default;
            }

            if (typeof(T) == typeof(DateTime))
            {
                return Date64Type.Default;
            }

            if (typeof(T) == typeof(TimeSpan))
            {
                return TimestampType.Default;
            }

            if (typeof(T) == typeof(string))
            {
                return StringType.Default;
            }

            if (typeof(T) == typeof(byte[]))
            {
                return BinaryType.Default;
            }

            throw new NotSupportedException($"Unknown type: {typeof(T)}");
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
            if (typeof(T) == typeof(bool))
            {
                return ToBooleanArray((bool[])(object)array);
            }

            if (typeof(T) == typeof(sbyte))
            {
                return ToPrimitiveArrowArray((sbyte[])(object)array);
            }

            if (typeof(T) == typeof(byte))
            {
                return ToPrimitiveArrowArray((byte[])(object)array);
            }

            if (typeof(T) == typeof(short))
            {
                return ToPrimitiveArrowArray((short[])(object)array);
            }

            if (typeof(T) == typeof(ushort))
            {
                return ToPrimitiveArrowArray((ushort[])(object)array);
            }

            if (typeof(T) == typeof(int))
            {
                return ToPrimitiveArrowArray((int[])(object)array);
            }

            if (typeof(T) == typeof(uint))
            {
                return ToPrimitiveArrowArray((uint[])(object)array);
            }

            if (typeof(T) == typeof(long))
            {
                return ToPrimitiveArrowArray((long[])(object)array);
            }

            if (typeof(T) == typeof(ulong))
            {
                return ToPrimitiveArrowArray((ulong[])(object)array);
            }

            if (typeof(T) == typeof(float))
            {
                return ToPrimitiveArrowArray((float[])(object)array);
            }

            if (typeof(T) == typeof(double))
            {
                return ToPrimitiveArrowArray((double[])(object)array);
            }

            if (typeof(T) == typeof(DateTime))
            {
                return ToPrimitiveArrowArray((DateTime[])(object)array);
            }

            if (typeof(T) == typeof(TimeSpan))
            {
                return ToPrimitiveArrowArray((TimeSpan[])(object)array);
            }

            if (typeof(T) == typeof(string))
            {
                return ToStringArrowArray((string[])(object)array);
            }

            if (typeof(T) == typeof(byte[]))
            {
                return ToBinaryArrowArray((byte[][])(object)array);
            }

            throw new NotSupportedException($"Unknown type: {typeof(T)}");
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
                var bytes = Encoding.UTF8.GetBytes(str);
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
