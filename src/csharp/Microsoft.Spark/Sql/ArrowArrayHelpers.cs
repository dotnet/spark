// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Helper methods to work with Apache Arrow arrays.
    /// </summary>
    internal static class ArrowArrayHelpers
    {
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
            var offset = 0;

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
            var offset = 0;

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

        public static Action<T> CreateArrowArray<T>(int length, out Func<IArrowArray> build)
        {
            if (typeof(T) == typeof(bool))
            {
                return (Action<T>)(object)CreateBooleanArray(length, out build);
            }

            if (typeof(T) == typeof(sbyte))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<sbyte>(length, out build);
            }

            if (typeof(T) == typeof(byte))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<byte>(length, out build);
            }

            if (typeof(T) == typeof(short))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<short>(length, out build);
            }

            if (typeof(T) == typeof(ushort))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<ushort>(length, out build);
            }

            if (typeof(T) == typeof(int))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<int>(length, out build);
            }

            if (typeof(T) == typeof(uint))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<uint>(length, out build);
            }

            if (typeof(T) == typeof(long))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<long>(length, out build);
            }

            if (typeof(T) == typeof(ulong))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<ulong>(length, out build);
            }

            if (typeof(T) == typeof(float))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<float>(length, out build);
            }

            if (typeof(T) == typeof(double))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<double>(length, out build);
            }

            if (typeof(T) == typeof(DateTime))
            {
                return (Action<T>)(object)CreatePrimitiveArrowArray<DateTime>(length, out build);
            }

            if (typeof(T) == typeof(string))
            {
                return (Action<T>)(object)CreateStringArray(length, out build);
            }

            if (typeof(T) == typeof(byte[]))
            {
                return (Action<T>)(object)CreateBinaryArray(length, out build);
            }

            throw new NotSupportedException($"Unknown type: {typeof(T)}");
        }

        private static Action<T> CreatePrimitiveArrowArray<T>(
            int length,
            out Func<IArrowArray> build) where T : struct
        {
            var builder = new ArrowBuffer.Builder<T>(length);
            build = () =>
            {
                var data = new ArrayData(
                    GetArrowType<T>(),
                    length,
                    0,
                    0,
                    new[] { ArrowBuffer.Empty, builder.Build() });

                IArrowArray result = ArrowArrayFactory.BuildArray(data);
                builder = null;
                return result;
            };
            return item => builder.Append(item);
        }

        private static Action<bool> CreateBooleanArray(
            int length,
            out Func<IArrowArray> build)
        {
            byte[] rawBytes = CreateRawBytesForBoolArray(length);
            build = () =>
            {
                var builder = new ArrowBuffer.Builder<byte>(rawBytes.Length);
                builder.AppendRange(rawBytes);

                var data = new ArrayData(
                    BooleanType.Default,
                    length,
                    0,
                    0,
                    new[] { ArrowBuffer.Empty, builder.Build() });

                return ArrowArrayFactory.BuildArray(data);
            };

            int currentIndex = 0;
            return item =>
            {
                // Only need to set true values since rawBytes is zeroed by the .NET runtime.
                if (item)
                {
                    BitUtility.SetBit(rawBytes, currentIndex);
                }
                ++currentIndex;
            };
        }

        private static Action<string> CreateStringArray(int length, out Func<IArrowArray> build)
        {
            var valueOffsets = new ArrowBuffer.Builder<int>();
            var valueBuffer = new ArrowBuffer.Builder<byte>();
            var offset = 0;

            build = () =>
            {
                valueOffsets.Append(offset);
                var result = new StringArray(
                    new ArrayData(
                        StringType.Default,
                        valueOffsets.Length - 1,
                        0,
                        0,
                        new[] { ArrowBuffer.Empty, valueOffsets.Build(), valueBuffer.Build() }));

                valueOffsets = null;
                valueBuffer = null;
                offset = 0;
                return result;
            };

            return str =>
            {
                var bytes = Encoding.UTF8.GetBytes(str);
                valueOffsets.Append(offset);
                // TODO: Anyway to use the span-based GetBytes to write directly to
                // the value buffer?
                valueBuffer.Append(bytes);
                offset += bytes.Length;
            };
        }

        private static Action<byte[]> CreateBinaryArray(int length, out Func<IArrowArray> build)
        {
            var valueOffsets = new ArrowBuffer.Builder<int>();
            var valueBuffer = new ArrowBuffer.Builder<byte>();
            var offset = 0;

            build = () =>
            {
                valueOffsets.Append(offset);
                var result = new BinaryArray(
                    new ArrayData(
                        BinaryType.Default,
                        valueOffsets.Length - 1,
                        0,
                        0,
                        new[] { ArrowBuffer.Empty, valueOffsets.Build(), valueBuffer.Build() }));

                valueOffsets = null;
                valueBuffer = null;
                offset = 0;
                return result;
            };

            return bytes =>
            {
                valueOffsets.Append(offset);
                valueBuffer.Append(bytes);
                offset += bytes.Length;
            };
        }

        public static Func<int, T> GetGetter<T>(IArrowArray array)
        {
            if (array is null)
            {
                return null;
            }

            // TODO: determine fastest way to read out a value from the array.

            if (typeof(T) == typeof(bool))
            {
                var booleanArray = new BooleanArray(array.Data);
                return (Func<int, T>)(object)new Func<int, bool>(
                    index => booleanArray.GetBoolean(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(bool?))
            {
                var booleanArray = new BooleanArray(array.Data);
                return (Func<int, T>)(object)new Func<int, bool?>(booleanArray.GetBoolean);
            }

            if (typeof(T) == typeof(sbyte))
            {
                var int8Array = new Int8Array(array.Data);
                return (Func<int, T>)(object)new Func<int, sbyte>(
                    index => int8Array.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(sbyte?))
            {
                var int8Array = new Int8Array(array.Data);
                return (Func<int, T>)(object)new Func<int, sbyte?>(int8Array.GetValue);
            }

            if (typeof(T) == typeof(byte))
            {
                var uint8Array = new UInt8Array(array.Data);
                return (Func<int, T>)(object)new Func<int, byte>(
                    index => uint8Array.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(byte?))
            {
                var uint8Array = new UInt8Array(array.Data);
                return (Func<int, T>)(object)new Func<int, byte?>(uint8Array.GetValue);
            }

            if (typeof(T) == typeof(short))
            {
                var int16Array = new Int16Array(array.Data);
                return (Func<int, T>)(object)new Func<int, short>(
                    index => int16Array.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(short?))
            {
                var int16Array = new Int16Array(array.Data);
                return (Func<int, T>)(object)new Func<int, short?>(int16Array.GetValue);
            }

            if (typeof(T) == typeof(ushort))
            {
                var uint16Array = new UInt16Array(array.Data);
                return (Func<int, T>)(object)new Func<int, ushort>(
                    index => uint16Array.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(ushort?))
            {
                var uint16Array = new UInt16Array(array.Data);
                return (Func<int, T>)(object)new Func<int, ushort?>(uint16Array.GetValue);
            }

            if (typeof(T) == typeof(int))
            {
                var int32Array = new Int32Array(array.Data);
                return (Func<int, T>)(object)new Func<int, int>(
                    index => int32Array.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(int?))
            {
                var int32Array = new Int32Array(array.Data);
                return (Func<int, T>)(object)new Func<int, int?>(int32Array.GetValue);
            }

            if (typeof(T) == typeof(uint))
            {
                var uint32Array = new UInt32Array(array.Data);
                return (Func<int, T>)(object)new Func<int, uint>(
                    index => uint32Array.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(uint?))
            {
                var uint32Array = new UInt32Array(array.Data);
                return (Func<int, T>)(object)new Func<int, uint?>(uint32Array.GetValue);
            }

            if (typeof(T) == typeof(long))
            {
                var int64Array = new Int64Array(array.Data);
                return (Func<int, T>)(object)new Func<int, long>(
                    index => int64Array.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(long?))
            {
                var int64Array = new Int64Array(array.Data);
                return (Func<int, T>)(object)new Func<int, long?>(int64Array.GetValue);
            }

            if (typeof(T) == typeof(ulong))
            {
                var uint64Array = new UInt64Array(array.Data);
                return (Func<int, T>)(object)new Func<int, ulong>(
                    index => uint64Array.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(ulong?))
            {
                var uint64Array = new UInt64Array(array.Data);
                return (Func<int, T>)(object)new Func<int, ulong?>(uint64Array.GetValue);
            }

            if (typeof(T) == typeof(double))
            {
                var doubleArray = new DoubleArray(array.Data);
                return (Func<int, T>)(object)new Func<int, double>(
                    index => doubleArray.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(double?))
            {
                var doubleArray = new DoubleArray(array.Data);
                return (Func<int, T>)(object)new Func<int, double?>(doubleArray.GetValue);
            }

            if (typeof(T) == typeof(float))
            {
                var floatArray = new FloatArray(array.Data);
                return (Func<int, T>)(object)new Func<int, float>(
                    index => floatArray.GetValue(index).GetValueOrDefault());
            }
            if (typeof(T) == typeof(float?))
            {
                var floatArray = new FloatArray(array.Data);
                return (Func<int, T>)(object)new Func<int, float?>(floatArray.GetValue);
            }

            if (typeof(T) == typeof(DateTime))
            {
                if (array.Data.DataType.TypeId == ArrowTypeId.Date32)
                {
                    var date32Array = new Date32Array(array.Data);
                    return (Func<int, T>)(object)new Func<int, DateTime>(
                        index => date32Array.GetDate(index).GetValueOrDefault().DateTime);
                }
                else if (array.Data.DataType.TypeId == ArrowTypeId.Date64)
                {
                    var date64Array = new Date64Array(array.Data);
                    return (Func<int, T>)(object)new Func<int, DateTime>(
                        index => date64Array.GetDate(index).GetValueOrDefault().DateTime);
                }
            }
            if (typeof(T) == typeof(DateTime?))
            {
                if (array.Data.DataType.TypeId == ArrowTypeId.Date32)
                {
                    var date32Array = new Date32Array(array.Data);
                    return (Func<int, T>)(object)new Func<int, DateTime?>(
                        index => date32Array.GetDate(index)?.DateTime);
                }
                else if (array.Data.DataType.TypeId == ArrowTypeId.Date64)
                {
                    var date64Array = new Date64Array(array.Data);
                    return (Func<int, T>)(object)new Func<int, DateTime?>(
                        index => date64Array.GetDate(index)?.DateTime);
                }
            }

            if (typeof(T) == typeof(DateTimeOffset))
            {
                if (array.Data.DataType.TypeId == ArrowTypeId.Date32)
                {
                    var date32Array = new Date32Array(array.Data);
                    return (Func<int, T>)(object)new Func<int, DateTimeOffset>(
                        index => date32Array.GetDate(index).GetValueOrDefault());
                }
                else if (array.Data.DataType.TypeId == ArrowTypeId.Date64)
                {
                    var date64Array = new Date64Array(array.Data);
                    return (Func<int, T>)(object)new Func<int, DateTimeOffset>(
                        index => date64Array.GetDate(index).GetValueOrDefault());
                }
            }
            if (typeof(T) == typeof(DateTimeOffset?))
            {
                if (array.Data.DataType.TypeId == ArrowTypeId.Date32)
                {
                    var date32Array = new Date32Array(array.Data);
                    return (Func<int, T>)(object)new Func<int, DateTimeOffset?>(
                        date32Array.GetDate);
                }
                else if (array.Data.DataType.TypeId == ArrowTypeId.Date64)
                {
                    var date64Array = new Date64Array(array.Data);
                    return (Func<int, T>)(object)new Func<int, DateTimeOffset?>(
                        date64Array.GetDate);
                }
            }

            if (typeof(T) == typeof(TimeSpan))
            {
                var timestampArray = new TimestampArray(array.Data);
                return (Func<int, T>)(object)new Func<int, TimeSpan>(
                    index => timestampArray.GetTimestamp(index).GetValueOrDefault().TimeOfDay);
            }
            if (typeof(T) == typeof(TimeSpan?))
            {
                var timestampArray = new TimestampArray(array.Data);
                return (Func<int, T>)(object)new Func<int, TimeSpan?>(
                    index => timestampArray.GetTimestamp(index)?.TimeOfDay);
            }

            if (typeof(T) == typeof(byte[]))
            {
                var binaryArray = new BinaryArray(array.Data);
                return (Func<int, T>)(object)new Func<int, byte[]>(
                    // TODO: how to avoid this allocation/copy?
                    index => binaryArray.GetBytes(index).ToArray());
            }

            if (typeof(T) == typeof(string))
            {
                var stringArray = new StringArray(array.Data);
                return (Func<int, T>)(object)new Func<int, string>(
                    index => stringArray.GetString(index));
            }

            // It's something else we don't yet support.
            switch (array.Data.DataType.TypeId)
            {
                case ArrowTypeId.Decimal:
                case ArrowTypeId.Dictionary:
                case ArrowTypeId.FixedSizedBinary:
                case ArrowTypeId.HalfFloat:
                case ArrowTypeId.Interval:
                case ArrowTypeId.List:
                case ArrowTypeId.Map:
                case ArrowTypeId.Null:
                case ArrowTypeId.Struct:
                case ArrowTypeId.Time32:
                case ArrowTypeId.Time64:
                case ArrowTypeId.Union:
                default:
                    // TODO: support additional types?
                    throw new NotSupportedException(
                        $"Not supported array type: {array.Data.DataType.TypeId}");
            }
        }
    }
}
