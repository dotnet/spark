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
            return typeof(T) switch
            {
                Type t when t == typeof(BooleanDataFrameColumn) =>
                    new BooleanDataFrameColumn("Empty"),
                Type t when t == typeof(SByteDataFrameColumn) =>
                    new SByteDataFrameColumn("Empty"),
                Type t when t == typeof(ByteDataFrameColumn) =>
                    new ByteDataFrameColumn("Empty"),
                Type t when t == typeof(Int16DataFrameColumn) =>
                    new Int16DataFrameColumn("Empty"),
                Type t when t == typeof(UInt16DataFrameColumn) =>
                    new UInt16DataFrameColumn("Empty"),
                Type t when t == typeof(Int32DataFrameColumn) =>
                    new Int32DataFrameColumn("Empty"),
                Type t when t == typeof(UInt32DataFrameColumn) =>
                    new UInt32DataFrameColumn("Empty"),
                Type t when t == typeof(Int64DataFrameColumn) =>
                    new Int64DataFrameColumn("Empty"),
                Type t when t == typeof(UInt64DataFrameColumn) =>
                    new UInt64DataFrameColumn("Empty"),
                Type t when t == typeof(SingleDataFrameColumn) =>
                    new SingleDataFrameColumn("Empty"),
                Type t when t == typeof(DoubleDataFrameColumn) =>
                    new DoubleDataFrameColumn("Empty"),
                Type t when t == typeof(ArrowStringDataFrameColumn) =>
                    new ArrowStringDataFrameColumn("Empty"),

                _ => throw new NotSupportedException($"Unknown type: {typeof(T)}")
            };
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
            return typeof(T) switch
            {
                Type t when t == typeof(BooleanArray) =>
                    BuildEmptyArrayDataFromArrowType(BooleanType.Default),
                Type t when t == typeof(Int8Array) =>
                    BuildEmptyArrayDataFromArrowType(Int8Type.Default),
                Type t when t == typeof(UInt8Array) =>
                    BuildEmptyArrayDataFromArrowType(UInt8Type.Default),
                Type t when t == typeof(Int16Array) =>
                    BuildEmptyArrayDataFromArrowType(Int16Type.Default),
                Type t when t == typeof(UInt16Array) =>
                    BuildEmptyArrayDataFromArrowType(UInt16Type.Default),
                Type t when t == typeof(Int32Array) =>
                    BuildEmptyArrayDataFromArrowType(Int32Type.Default),
                Type t when t == typeof(UInt32Array) =>
                    BuildEmptyArrayDataFromArrowType(UInt32Type.Default),
                Type t when t == typeof(Int64Array) =>
                    BuildEmptyArrayDataFromArrowType(Int64Type.Default),
                Type t when t == typeof(UInt64Array) =>
                    BuildEmptyArrayDataFromArrowType(UInt64Type.Default),
                Type t when t == typeof(FloatArray) =>
                    BuildEmptyArrayDataFromArrowType(FloatType.Default),
                Type t when t == typeof(DoubleArray) =>
                    BuildEmptyArrayDataFromArrowType(DoubleType.Default),
                Type t when t == typeof(Date64Array) =>
                    BuildEmptyArrayDataFromArrowType(Date64Type.Default),
                Type t when t == typeof(TimestampArray) =>
                    BuildEmptyArrayDataFromArrowType(TimestampType.Default),
                Type t when t == typeof(StringArray) =>
                    BuildEmptyArrayDataFromArrowType(StringType.Default),
                Type t when t == typeof(BinaryArray) =>
                    BuildEmptyArrayDataFromArrowType(BinaryType.Default),

                _ => throw new NotSupportedException($"Unknown type: {typeof(T)}")
            };
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
