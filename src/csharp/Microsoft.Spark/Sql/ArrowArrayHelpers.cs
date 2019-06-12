// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Helper methods to work with Apache Arrow arrays.
    /// </summary>
    internal static class ArrowArrayHelpers
    {
        public static IArrowArray CreateEmptyArray<T>()
        {
            ArrayData data = BuildEmptyArrayDataFromArrayType<T>();
            return ArrowArrayFactory.BuildArray(data);
        }

        public static IArrowArray CreateEmptyArray(IArrowType arrowType)
        {
            ArrayData data = BuildEmptyArrayDataFromArrowType(arrowType);
            return ArrowArrayFactory.BuildArray(data);
        }

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

        private static ArrayData BuildEmptyArrayDataFromArrayType<T>()
        {
            IArrowType arrowType = null;

            if (typeof(T) == typeof(BooleanArray))
            {
                arrowType = BooleanType.Default;
            }
            else if (typeof(T) == typeof(Int8Array))
            {
                arrowType = Int8Type.Default;
            }
            else if (typeof(T) == typeof(UInt8Array))
            {
                arrowType = UInt8Type.Default;
            }
            else if (typeof(T) == typeof(Int16Array))
            {
                arrowType = Int16Type.Default;
            }
            else if (typeof(T) == typeof(UInt16Array))
            {
                arrowType = UInt16Type.Default;
            }
            else if (typeof(T) == typeof(Int32Array))
            {
                arrowType = Int32Type.Default;
            }
            else if (typeof(T) == typeof(UInt32Array))
            {
                arrowType = UInt32Type.Default;
            }
            else if (typeof(T) == typeof(Int64Array))
            {
                arrowType = Int64Type.Default;
            }
            else if (typeof(T) == typeof(UInt64Array))
            {
                arrowType = UInt64Type.Default;
            }
            else if (typeof(T) == typeof(FloatArray))
            {
                arrowType = FloatType.Default;
            }
            else if (typeof(T) == typeof(DoubleArray))
            {
                arrowType = DoubleType.Default;
            }
            else if (typeof(T) == typeof(Date64Array))
            {
                arrowType = Date64Type.Default;
            }
            else if (typeof(T) == typeof(TimestampArray))
            {
                arrowType = TimestampType.Default;
            }

            if (arrowType != null)
            {
                return new ArrayData(arrowType, 0,
                    buffers: new[] { ArrowBuffer.Empty, ArrowBuffer.Empty });
            }

            if (typeof(T) == typeof(StringArray))
            {
                return new ArrayData(StringType.Default, 0,
                    buffers: new[] { ArrowBuffer.Empty, ArrowBuffer.Empty, ArrowBuffer.Empty });
            }
            else if (typeof(T) == typeof(BinaryArray))
            {
                return new ArrayData(BinaryType.Default, 0,
                    buffers: new[] { ArrowBuffer.Empty, ArrowBuffer.Empty, ArrowBuffer.Empty });
            }

            throw new NotSupportedException($"Unknown type: {typeof(T)}");
        }
    }
}
