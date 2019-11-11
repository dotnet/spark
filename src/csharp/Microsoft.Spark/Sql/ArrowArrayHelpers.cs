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
            DataFrameColumn ret;
            if (typeof(T) == typeof(PrimitiveDataFrameColumn<bool>))
            {
                ret = new PrimitiveDataFrameColumn<bool>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<sbyte>))
            {
                ret = new PrimitiveDataFrameColumn<sbyte>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<byte>))
            {
                ret = new PrimitiveDataFrameColumn<byte>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<short>))
            {
                ret = new PrimitiveDataFrameColumn<short>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<ushort>))
            {
                ret = new PrimitiveDataFrameColumn<ushort>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<int>))
            {
                ret = new PrimitiveDataFrameColumn<int>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<uint>))
            {
                ret = new PrimitiveDataFrameColumn<uint>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<long>))
            {
                ret = new PrimitiveDataFrameColumn<long>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<ulong>))
            {
                ret = new PrimitiveDataFrameColumn<ulong>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<float>))
            {
                ret = new PrimitiveDataFrameColumn<float>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveDataFrameColumn<double>))
            {
                ret = new PrimitiveDataFrameColumn<double>("Empty");
            }
            else if (typeof(T) == typeof(ArrowStringDataFrameColumn))
            {
                ret = new ArrowStringDataFrameColumn("Empty");
            }
            else
                throw new NotSupportedException($"Unknown type: {typeof(T)}");

            return ret;
        }

        public static IArrowArray CreateEmptyArray(IArrowType arrowType)
        {
            ArrayData data = BuildEmptyArrayDataFromArrowType(arrowType);
            return ArrowArrayFactory.BuildArray(data);
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
