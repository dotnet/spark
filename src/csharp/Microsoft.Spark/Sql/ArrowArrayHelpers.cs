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
            return type switch
            {
                _ when type == typeof(BooleanDataFrameColumn) =>
                    new BooleanDataFrameColumn("Empty"),
                _ when type == typeof(SByteDataFrameColumn) =>
                    new SByteDataFrameColumn("Empty"),
                _ when type == typeof(ByteDataFrameColumn) =>
                    new ByteDataFrameColumn("Empty"),
                _ when type == typeof(Int16DataFrameColumn) =>
                    new Int16DataFrameColumn("Empty"),
                _ when type == typeof(UInt16DataFrameColumn) =>
                    new UInt16DataFrameColumn("Empty"),
                _ when type == typeof(Int32DataFrameColumn) =>
                    new Int32DataFrameColumn("Empty"),
                _ when type == typeof(UInt32DataFrameColumn) =>
                    new UInt32DataFrameColumn("Empty"),
                _ when type == typeof(Int64DataFrameColumn) =>
                    new Int64DataFrameColumn("Empty"),
                _ when type == typeof(UInt64DataFrameColumn) =>
                    new UInt64DataFrameColumn("Empty"),
                _ when type == typeof(SingleDataFrameColumn) =>
                    new SingleDataFrameColumn("Empty"),
                _ when type == typeof(DoubleDataFrameColumn) =>
                    new DoubleDataFrameColumn("Empty"),
                _ when type == typeof(ArrowStringDataFrameColumn) =>
                    new ArrowStringDataFrameColumn("Empty"),
                _ => throw new NotSupportedException($"Unknown type: {type}")
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
            Type type = typeof(T);
            return type switch
            {
                _ when type == typeof(BooleanArray) =>
                    BuildEmptyArrayDataFromArrowType(BooleanType.Default),
                _ when type == typeof(Int8Array) =>
                    BuildEmptyArrayDataFromArrowType(Int8Type.Default),
                _ when type == typeof(UInt8Array) =>
                    BuildEmptyArrayDataFromArrowType(UInt8Type.Default),
                _ when type == typeof(Int16Array) =>
                    BuildEmptyArrayDataFromArrowType(Int16Type.Default),
                _ when type == typeof(UInt16Array) =>
                    BuildEmptyArrayDataFromArrowType(UInt16Type.Default),
                _ when type == typeof(Int32Array) =>
                    BuildEmptyArrayDataFromArrowType(Int32Type.Default),
                _ when type == typeof(UInt32Array) =>
                    BuildEmptyArrayDataFromArrowType(UInt32Type.Default),
                _ when type == typeof(Int64Array) =>
                    BuildEmptyArrayDataFromArrowType(Int64Type.Default),
                _ when type == typeof(UInt64Array) =>
                    BuildEmptyArrayDataFromArrowType(UInt64Type.Default),
                _ when type == typeof(FloatArray) =>
                    BuildEmptyArrayDataFromArrowType(FloatType.Default),
                _ when type == typeof(DoubleArray) =>
                    BuildEmptyArrayDataFromArrowType(DoubleType.Default),
                _ when type == typeof(Date64Array) =>
                    BuildEmptyArrayDataFromArrowType(Date64Type.Default),
                _ when type == typeof(TimestampArray) =>
                    BuildEmptyArrayDataFromArrowType(TimestampType.Default),
                _ when type == typeof(StringArray) =>
                    BuildEmptyArrayDataFromArrowType(StringType.Default),
                _ when type == typeof(BinaryArray) =>
                    BuildEmptyArrayDataFromArrowType(BinaryType.Default),

                _ => throw new NotSupportedException($"Unknown type: {type}")
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
