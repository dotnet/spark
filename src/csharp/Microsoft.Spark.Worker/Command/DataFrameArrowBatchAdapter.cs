// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;

namespace Microsoft.Spark.Worker.Command
{
    /// <summary>
    /// Adds bounded Arrow conversion for regular string columns without mutating UDF results.
    /// </summary>
    internal static class DataFrameArrowBatchAdapter
    {
        internal const int MaxRowsPerBatch = 65_536;
        internal const int MaxStringBytesPerBatch = 8 * 1024 * 1024;

        internal static IEnumerable<PreparedArrowBatch> Convert(DataFrame result) =>
            Convert(result, MaxRowsPerBatch, MaxStringBytesPerBatch, length => new byte[length]);

        internal static IEnumerable<PreparedArrowBatch> Convert(
            DataFrame result,
            int maxRows,
            int maxStringBytes,
            Func<int, byte[]> allocateBuffer)
        {
            if (result == null)
            {
                throw new ArgumentException("The grouped UDF returned a null DataFrame.", nameof(result));
            }

            if (result.Columns.Count == 0)
            {
                throw new ArgumentException("The grouped UDF returned a DataFrame with no columns.", nameof(result));
            }

            if (maxRows <= 0 || maxRows > MaxRowsPerBatch)
            {
                throw new ArgumentOutOfRangeException(nameof(maxRows));
            }

            if (maxStringBytes <= 0 || maxStringBytes > MaxStringBytesPerBatch)
            {
                throw new ArgumentOutOfRangeException(nameof(maxStringBytes));
            }

            if (allocateBuffer == null)
            {
                throw new ArgumentNullException(nameof(allocateBuffer));
            }

            bool hasRegularString = false;
            foreach (DataFrameColumn column in result.Columns)
            {
                if (column.Length != result.Rows.Count)
                {
                    throw new ArgumentException($"Column '{column.Name}' has an inconsistent length.", nameof(result));
                }

                hasRegularString |= column is StringDataFrameColumn;
            }

            if (!hasRegularString)
            {
                // Preserve existing export behavior for results outside the new mixed-string path.
                foreach (RecordBatch batch in result.ToArrowRecordBatches())
                {
                    using (PreparedArrowBatch prepared = PreparedArrowBatch.Own(batch))
                    {
                        prepared.Hold(result);
                        yield return prepared;
                    }
                }

                yield break;
            }

            var strings = new List<StringColumn>();
            var numericIndices = new List<int>();
            try
            {
                for (int i = 0; i < result.Columns.Count; ++i)
                {
                    DataFrameColumn column = result.Columns[i];
                    Type type = column.GetType();
                    if (type == typeof(StringDataFrameColumn) || type == typeof(ArrowStringDataFrameColumn))
                    {
                        strings.Add(new StringColumn(i, column));
                    }
                    else if (IsBuiltInPrimitive<bool, BooleanDataFrameColumn>(type))
                    {
                        // Booleans need bit-packed buffers, unlike MDA's byte-per-value storage.
                    }
                    else if (GetNumericType(type) != null)
                    {
                        numericIndices.Add(i);
                    }
                    else
                    {
                        throw new NotSupportedException(
                            $"Column '{column.Name}' of type '{type.FullName}' cannot be exported " +
                            "with a StringDataFrameColumn. Supported primitive types are " +
                            "Boolean, SByte, Int16, Int32, Int64, Single and Double.");
                    }
                }

                long start = 0;
                do
                {
                    var byteCounts = new int[strings.Count];
                    int count = PlanWindow(result.Rows.Count, start, strings, byteCounts, maxRows, maxStringBytes);
                    using (PreparedArrowBatch prepared = PrepareWindow(
                        result, start, count, strings, byteCounts, numericIndices, allocateBuffer))
                    {
                        yield return prepared;
                    }

                    start = checked(start + count);
                }
                while (start < result.Rows.Count);
            }
            finally
            {
                foreach (StringColumn column in strings)
                {
                    column.Dispose();
                }
            }
        }

        internal static int OffsetBufferLength(int count) => checked(checked(count + 1) * sizeof(int));

        internal static Int64DataFrameColumn CreateRowMap(long start, int count)
        {
            if (start < 0 || count < 0)
            {
                throw new ArgumentOutOfRangeException();
            }

            checked
            {
                _ = start + count;
            }

            var map = new Int64DataFrameColumn("__arrow_window_indices", count);
            for (int i = 0; i < count; ++i)
            {
                map[i] = checked(start + i);
            }

            return map;
        }

        private static int PlanWindow(
            long totalRows,
            long start,
            List<StringColumn> strings,
            int[] byteCounts,
            int maxRows,
            int maxBytes)
        {
            int count = 0;
            long totalBytes = 0;
            var rowBytes = new int[strings.Count];
            while (count < maxRows && count < totalRows - start)
            {
                long row = checked(start + count);
                long rowTotal = 0;
                for (int i = 0; i < strings.Count; ++i)
                {
                    rowBytes[i] = strings[i].GetByteCount(row);
                    rowTotal = checked(rowTotal + rowBytes[i]);
                }

                if (rowTotal > maxBytes)
                {
                    throw new InvalidOperationException(
                        $"Row {row} requires {rowTotal} UTF-8 string bytes, exceeding the " +
                        $"Arrow batch limit of {maxBytes} bytes.");
                }

                if (totalBytes + rowTotal > maxBytes)
                {
                    break;
                }

                for (int i = 0; i < strings.Count; ++i)
                {
                    byteCounts[i] = checked(byteCounts[i] + rowBytes[i]);
                    strings[i].AdvancePreflight();
                }

                totalBytes += rowTotal;
                ++count;
            }

            return count;
        }

        private static PreparedArrowBatch PrepareWindow(
            DataFrame result,
            long start,
            int count,
            List<StringColumn> strings,
            int[] byteCounts,
            List<int> numericIndices,
            Func<int, byte[]> allocateBuffer)
        {
            var prepared = new PreparedArrowBatch();
            try
            {
                var arrays = new IArrowArray[result.Columns.Count];
                var fields = new Field[result.Columns.Count];
                for (int i = 0; i < strings.Count; ++i)
                {
                    StringColumn column = strings[i];
                    StringArray array = BuildStringArray(column, start, count, byteCounts[i], allocateBuffer, prepared);
                    prepared.AddOwned(array);
                    arrays[column.Index] = array;
                }

                for (int i = 0; i < result.Columns.Count; ++i)
                {
                    if (IsBuiltInPrimitive<bool, BooleanDataFrameColumn>(result.Columns[i].GetType()))
                    {
                        BooleanArray array = BuildBooleanArray(
                            (PrimitiveDataFrameColumn<bool>)result.Columns[i], start, count, allocateBuffer, prepared);
                        prepared.AddOwned(array);
                        arrays[i] = array;
                    }
                }

                if (numericIndices.Count != 0)
                {
                    if (count == 0)
                    {
                        // MDA Clone on an empty primitive source accesses a nonexistent buffer.
                        foreach (int index in numericIndices)
                        {
                            IArrowArray array = ArrowArrayFactory.BuildArray(new ArrayData(
                                GetNumericType(result.Columns[index].GetType()), 0, 0, 0,
                                new[] { ArrowBuffer.Empty, ArrowBuffer.Empty }));
                            prepared.AddOwned(array);
                            arrays[index] = array;
                        }
                    }
                    else
                    {
                        Int64DataFrameColumn map = CreateRowMap(start, count);
                        var columns = new List<DataFrameColumn>(numericIndices.Count);
                        foreach (int index in numericIndices)
                        {
                            columns.Add(result.Columns[index].Clone(map));
                        }

                        var numericFrame = new DataFrame(columns);
                        prepared.Hold(numericFrame);
                        using (IEnumerator<RecordBatch> batches = numericFrame.ToArrowRecordBatches().GetEnumerator())
                        {
                            if (!batches.MoveNext())
                            {
                                throw new InvalidOperationException("The numeric Arrow window produced no batch.");
                            }

                            RecordBatch numericBatch = batches.Current;
                            for (int i = 0; i < numericIndices.Count; ++i)
                            {
                                IArrowArray array = numericBatch.Column(i);
                                prepared.AddOwned(array);
                                arrays[numericIndices[i]] = array;
                            }

                            bool extraBatch = batches.MoveNext();
                            if (extraBatch)
                            {
                                foreach (IArrowArray array in batches.Current.Arrays)
                                {
                                    prepared.AddOwned(array);
                                }
                            }

                            if (numericBatch.Length != count || extraBatch)
                            {
                                throw new InvalidOperationException("The numeric Arrow window did not produce exactly one complete batch.");
                            }
                        }
                    }
                }

                for (int i = 0; i < arrays.Length; ++i)
                {
                    fields[i] = new Field(result.Columns[i].Name, arrays[i].Data.DataType, arrays[i].NullCount != 0);
                }

                prepared.ReplaceBatch(new RecordBatch(new Schema(fields, null), arrays, count));
                return prepared;
            }
            catch
            {
                prepared.DisposeAfterFailure();
                throw;
            }
        }

        private static StringArray BuildStringArray(
            StringColumn column,
            long start,
            int count,
            int byteCount,
            Func<int, byte[]> allocateBuffer,
            PreparedArrowBatch prepared)
        {
            byte[] values = Allocate(byteCount, allocateBuffer, prepared);
            byte[] offsets = Allocate(OffsetBufferLength(count), allocateBuffer, prepared);
            byte[] validity = Allocate((count + 7) / 8, allocateBuffer, prepared);
            int offset = 0;
            int nullCount = 0;
            for (int i = 0; i < count; ++i)
            {
                bool valid;
                int written = column.Encode(checked(start + i), values, offset, out valid);
                if (valid)
                {
                    SetBit(validity, i);
                }
                else
                {
                    ++nullCount;
                }

                offset = checked(offset + written);
                WriteOffset(offsets, i + 1, offset);
            }

            if (offset != byteCount)
            {
                throw new InvalidOperationException("The string column changed during Arrow conversion.");
            }

            return new StringArray(count, new ArrowBuffer(offsets), new ArrowBuffer(values),
                new ArrowBuffer(validity), nullCount, 0);
        }

        private static BooleanArray BuildBooleanArray(
            PrimitiveDataFrameColumn<bool> column,
            long start,
            int count,
            Func<int, byte[]> allocateBuffer,
            PreparedArrowBatch prepared)
        {
            int bitmapLength = (count + 7) / 8;
            byte[] values = Allocate(bitmapLength, allocateBuffer, prepared);
            byte[] validity = Allocate(bitmapLength, allocateBuffer, prepared);
            int nullCount = 0;
            for (int i = 0; i < count; ++i)
            {
                bool? value = column[checked(start + i)];
                if (value.HasValue)
                {
                    SetBit(validity, i);
                    if (value.Value)
                    {
                        SetBit(values, i);
                    }
                }
                else
                {
                    ++nullCount;
                }
            }

            return new BooleanArray(new ArrowBuffer(values), new ArrowBuffer(validity), count, nullCount, 0);
        }

        private static byte[] Allocate(int length, Func<int, byte[]> allocateBuffer, PreparedArrowBatch prepared)
        {
            byte[] buffer = allocateBuffer(length);
            if (buffer == null || buffer.Length != length)
            {
                throw new InvalidOperationException("The Arrow buffer factory returned an invalid buffer.");
            }

            prepared.Hold(buffer);
            return buffer;
        }

        private static void SetBit(byte[] bitmap, int index) => bitmap[index / 8] |= (byte)(1 << (index & 7));

        private static void WriteOffset(byte[] offsets, int index, int value)
        {
            int position = checked(index * sizeof(int));
            offsets[position] = (byte)value;
            offsets[position + 1] = (byte)(value >> 8);
            offsets[position + 2] = (byte)(value >> 16);
            offsets[position + 3] = (byte)(value >> 24);
        }

        private static bool IsBuiltInPrimitive<T, TColumn>(Type type) where T : unmanaged =>
            type == typeof(TColumn) || type == typeof(PrimitiveDataFrameColumn<T>);

        private static IArrowType GetNumericType(Type type)
        {
            if (IsBuiltInPrimitive<sbyte, SByteDataFrameColumn>(type)) { return Int8Type.Default; }
            if (IsBuiltInPrimitive<short, Int16DataFrameColumn>(type)) { return Int16Type.Default; }
            if (IsBuiltInPrimitive<int, Int32DataFrameColumn>(type)) { return Int32Type.Default; }
            if (IsBuiltInPrimitive<long, Int64DataFrameColumn>(type)) { return Int64Type.Default; }
            if (IsBuiltInPrimitive<float, SingleDataFrameColumn>(type)) { return FloatType.Default; }
            if (IsBuiltInPrimitive<double, DoubleDataFrameColumn>(type)) { return DoubleType.Default; }
            return null;
        }

        private sealed class StringColumn : IDisposable
        {
            private readonly StringDataFrameColumn _regular;
            private readonly ArrowStringCursor _preflight;
            private readonly ArrowStringCursor _encode;

            internal StringColumn(int index, DataFrameColumn column)
            {
                Index = index;
                _regular = column as StringDataFrameColumn;
                if (_regular == null)
                {
                    _preflight = new ArrowStringCursor((ArrowStringDataFrameColumn)column);
                    _encode = new ArrowStringCursor((ArrowStringDataFrameColumn)column);
                }
            }

            internal int Index { get; }

            internal int GetByteCount(long row)
            {
                if (_regular != null)
                {
                    string value = _regular[row];
                    return value == null ? 0 : Encoding.UTF8.GetByteCount(value);
                }

                return _preflight.Peek(out bool _).Length;
            }

            internal void AdvancePreflight() => _preflight?.Advance();

            internal int Encode(long row, byte[] destination, int offset, out bool valid)
            {
                if (_regular != null)
                {
                    string value = _regular[row];
                    valid = value != null;
                    return valid ? Encoding.UTF8.GetBytes(value, 0, value.Length, destination, offset) : 0;
                }

                ReadOnlyMemory<byte> bytes = _encode.Peek(out valid);
                bytes.Span.CopyTo(destination.AsSpan(offset));
                _encode.Advance();
                return bytes.Length;
            }

            public void Dispose()
            {
                _preflight?.Dispose();
                _encode?.Dispose();
            }
        }

        /// <summary>
        /// Sequentially borrows one source buffer at a time. It never decodes Arrow UTF-8 strings.
        /// </summary>
        internal sealed class ArrowStringCursor : IDisposable
        {
            private readonly IEnumerator<ReadOnlyMemory<byte>> _values;
            private readonly IEnumerator<ReadOnlyMemory<int>> _offsets;
            private readonly IEnumerator<ReadOnlyMemory<byte>> _validity;
            private readonly bool _allValid;
            private int _row;
            private bool _loaded;

            internal ArrowStringCursor(ArrowStringDataFrameColumn column)
                : this(column.GetReadOnlyDataBuffers(), column.GetReadOnlyOffsetsBuffers(),
                    column.GetReadOnlyNullBitMapBuffers(), column.NullCount == 0)
            {
            }

            internal ArrowStringCursor(
                IEnumerable<ReadOnlyMemory<byte>> values,
                IEnumerable<ReadOnlyMemory<int>> offsets,
                IEnumerable<ReadOnlyMemory<byte>> validity,
                bool allValid)
            {
                _values = values.GetEnumerator();
                _offsets = offsets.GetEnumerator();
                _validity = validity.GetEnumerator();
                _allValid = allValid;
            }

            internal ReadOnlyMemory<byte> Peek(out bool valid)
            {
                while (!_loaded || _row == _offsets.Current.Length - 1)
                {
                    if (!_offsets.MoveNext() || !_values.MoveNext() || _offsets.Current.Length == 0)
                    {
                        throw new InvalidOperationException("The Arrow string column contains inconsistent buffers.");
                    }

                    if (!_validity.MoveNext() && !_allValid)
                    {
                        throw new InvalidOperationException("The Arrow string column is missing its validity buffer.");
                    }

                    _loaded = true;
                    _row = 0;
                }

                ReadOnlySpan<int> offsets = _offsets.Current.Span;
                int first = offsets[_row];
                int last = offsets[_row + 1];
                if (first < 0 || last < first || last > _values.Current.Length ||
                    (!_allValid && _row / 8 >= _validity.Current.Length))
                {
                    throw new InvalidOperationException("The Arrow string column contains invalid offsets or validity bits.");
                }

                valid = _allValid || (_validity.Current.Span[_row / 8] & (1 << (_row & 7))) != 0;
                return valid ? _values.Current.Slice(first, last - first) : ReadOnlyMemory<byte>.Empty;
            }

            internal void Advance() => ++_row;

            public void Dispose()
            {
                _values.Dispose();
                _offsets.Dispose();
                _validity.Dispose();
            }
        }
    }
}
