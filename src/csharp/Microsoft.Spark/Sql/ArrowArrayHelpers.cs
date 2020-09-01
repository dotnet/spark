// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Helper methods to work with Apache Arrow arrays.
    /// </summary>
    internal static class ArrowArrayHelpers
    {
        private static readonly HashSet<ArrowTypeId> s_twoBufferArrowTypes = new HashSet<ArrowTypeId>()
        {
            ArrowTypeId.Boolean,
            ArrowTypeId.Int8,
            ArrowTypeId.UInt8,
            ArrowTypeId.Int16,
            ArrowTypeId.UInt16,
            ArrowTypeId.Int32,
            ArrowTypeId.UInt32,
            ArrowTypeId.Int64,
            ArrowTypeId.UInt64,
            ArrowTypeId.Float,
            ArrowTypeId.Double,
            ArrowTypeId.Date32,
            ArrowTypeId.Date64,
            ArrowTypeId.Timestamp,
        };

        private static readonly HashSet<ArrowTypeId> s_threeBufferArrowTypes = new HashSet<ArrowTypeId>()
        {
            ArrowTypeId.String,
            ArrowTypeId.Binary,
        };

        public static DataFrameColumn CreateEmptyColumn<T>()
        {
            Type type = typeof(T);
            switch (type)
            {
                case Type t when t == typeof(BooleanDataFrameColumn):
                        return new BooleanDataFrameColumn("Empty");
                case Type t when t == typeof(SByteDataFrameColumn):
                    return new SByteDataFrameColumn("Empty");
                case Type t when t == typeof(ByteDataFrameColumn):
                    return new ByteDataFrameColumn("Empty");
                case Type t when t == typeof(Int16DataFrameColumn):
                    return new Int16DataFrameColumn("Empty");
                case Type t when t == typeof(UInt16DataFrameColumn):
                    return new UInt16DataFrameColumn("Empty");
                case Type t when t == typeof(Int32DataFrameColumn):
                    return new Int32DataFrameColumn("Empty");
                case Type t when t == typeof(UInt32DataFrameColumn):
                    return new UInt32DataFrameColumn("Empty");
                case Type t when t == typeof(Int64DataFrameColumn):
                    return new Int64DataFrameColumn("Empty");
                case Type t when t == typeof(UInt64DataFrameColumn):
                    return new UInt64DataFrameColumn("Empty");
                case Type t when t == typeof(SingleDataFrameColumn):
                    return new SingleDataFrameColumn("Empty");
                case Type t when t == typeof(DoubleDataFrameColumn):
                    return new DoubleDataFrameColumn("Empty");
                case Type t when t == typeof(ArrowStringDataFrameColumn):
                    return new ArrowStringDataFrameColumn("Empty");
                default:
                    throw new NotSupportedException($"Unknown type: {typeof(T)}");
            }
        }

        public static IArrowArray CreateEmptyArray(IArrowType arrowType)
        {
            ArrayData data = BuildEmptyArrayDataFromArrowType(arrowType);
            return ArrowArrayFactory.BuildArray(data);
        }

        public static IArrowArray CreateEmptyArray<T>()
        {
            ArrayData data = BuildEmptyArrayDataFromArrayType<T>();
            return ArrowArrayFactory.BuildArray(data);
        }

        private static ArrayData BuildEmptyArrayDataFromArrayType<T>()
        {
            IArrowType arrowType = null;

            Type type = typeof(T);
            switch (type)
            {
                case Type t when t == typeof(BooleanArray):
                    arrowType = BooleanType.Default;
                    break;
                case Type t when t == typeof(Int8Array):
                    arrowType = Int8Type.Default;
                    break;
                case Type t when t == typeof(UInt8Array):
                    arrowType = UInt8Type.Default;
                    break;
                case Type t when t == typeof(Int16Array):
                    arrowType = Int16Type.Default;
                    break;
                case Type t when t == typeof(UInt16Array):
                    arrowType = UInt16Type.Default;
                    break;
                case Type t when t == typeof(Int32Array):
                    arrowType = Int32Type.Default;
                    break;
                case Type t when t == typeof(UInt32Array):
                    arrowType = UInt32Type.Default;
                    break;
                case Type t when t == typeof(Int64Array):
                    arrowType = Int64Type.Default;
                    break;
                case Type t when t == typeof(UInt64Array):
                    arrowType = UInt64Type.Default;
                    break;
                case Type t when t == typeof(FloatArray):
                    arrowType = FloatType.Default;
                    break;
                case Type t when t == typeof(DoubleArray):
                    arrowType = DoubleType.Default;
                    break;
                case Type t when t == typeof(Date64Array):
                    arrowType = Date64Type.Default;
                    break;
                case Type t when t == typeof(TimestampArray):
                    arrowType = TimestampType.Default;
                    break;
                case Type t when t == typeof(StringArray):
                    return new ArrayData(StringType.Default, 0,
                    buffers: new[] { ArrowBuffer.Empty, ArrowBuffer.Empty, ArrowBuffer.Empty });
                case Type t when t == typeof(BinaryArray):
                    return new ArrayData(BinaryType.Default, 0,
                    buffers: new[] { ArrowBuffer.Empty, ArrowBuffer.Empty, ArrowBuffer.Empty });
            }

            if (arrowType != null)
            {
                return new ArrayData(arrowType, 0,
                    buffers: new[] { ArrowBuffer.Empty, ArrowBuffer.Empty });
            }

            throw new NotSupportedException($"Unknown type: {type}");
        }

        private static ArrayData BuildEmptyArrayDataFromArrowType(IArrowType arrowType)
        {
            if (s_twoBufferArrowTypes.Contains(arrowType.TypeId))
            {
                return new ArrayData(arrowType, 0,
                    buffers: new[] { ArrowBuffer.Empty, ArrowBuffer.Empty });
            }

            if (s_threeBufferArrowTypes.Contains(arrowType.TypeId))
            {
                return new ArrayData(arrowType, 0,
                    buffers: new[] { ArrowBuffer.Empty, ArrowBuffer.Empty, ArrowBuffer.Empty });
            }

            throw new NotSupportedException($"Unsupported type: {arrowType.TypeId}");
        }
    }
}
