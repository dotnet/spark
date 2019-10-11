// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data;

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

        public static BaseColumn CreateEmptyColumn<T>()
        {
            BaseColumn ret;
            if (typeof(T) == typeof(PrimitiveColumn<bool>))
            {
                ret = new PrimitiveColumn<bool>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<sbyte>))
            {
                ret = new PrimitiveColumn<sbyte>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<byte>))
            {
                ret = new PrimitiveColumn<byte>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<short>))
            {
                ret = new PrimitiveColumn<short>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<ushort>))
            {
                ret = new PrimitiveColumn<ushort>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<int>))
            {
                ret = new PrimitiveColumn<int>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<uint>))
            {
                ret = new PrimitiveColumn<uint>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<long>))
            {
                ret = new PrimitiveColumn<long>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<ulong>))
            {
                ret = new PrimitiveColumn<ulong>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<float>))
            {
                ret = new PrimitiveColumn<float>("Empty");
            }
            else if (typeof(T) == typeof(PrimitiveColumn<double>))
            {
                ret = new PrimitiveColumn<double>("Empty");
            }
            else if (typeof(T) == typeof(ArrowStringColumn))
            {
                ret = new ArrowStringColumn("Empty");
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
