// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Worker.Command;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class DataFrameArrowBatchAdapterTests
    {
        [Fact]
        public void MixedColumnsPreserveValuesAndInput()
        {
            string[] text = { null, "", "中文", "🙂", "abc", "\ud800" };
            string[] arrowText = { "arrow-0", null, "", "已有字符串", "🙂", "last" };
            bool?[] flags = { true, true, null, false, true, false };
            var frame = new DataFrame(
                new StringDataFrameColumn("description", text),
                new SByteDataFrameColumn("i8", new sbyte?[] { 1, null, -3, 4, 5, 6 }),
                MakeArrowColumn("arrow", arrowText),
                new Int16DataFrameColumn("i16", new short?[] { 10, 20, null, -40, 50, 60 }),
                new BooleanDataFrameColumn("flag", flags),
                new Int32DataFrameColumn("i32", new int?[] { 100, 200, 300, null, -500, 600 }),
                new Int64DataFrameColumn("i64", new long?[] { 1000, 2000, 3000, 4000, null, -6000 }),
                new SingleDataFrameColumn("f32", new float?[] { 1.5f, 2.5f, 3.5f, 4.5f, 5.5f, null }),
                new DoubleDataFrameColumn("f64", new double?[] { null, 2.25, 3.25, 4.25, 5.25, 6.25 }));
            object[][] input = ReadFrame(frame);
            var output = new List<object[]>();
            foreach (PreparedArrowBatch prepared in DataFrameArrowBatchAdapter.Convert(frame, 3, 64, NewBuffer))
            {
                Assert.Equal(frame.Columns.Select(column => column.Name),
                    prepared.Batch.Schema.FieldsList.Select(field => field.Name));
                output.AddRange(RoundTrip(prepared.Batch));
            }

            // Encoding.UTF8's normal replacement fallback is preserved for invalid UTF-16.
            object[][] expected = input.Select(row => (object[])row.Clone()).ToArray();
            expected[5][0] = "�";
            AssertRows(expected, output);
            AssertRows(input, ReadFrame(frame));
        }

        [Fact]
        public void EmptyResultsHaveTypedSchemaAndOneZeroStringOffset()
        {
            var frame = new DataFrame(
                new StringDataFrameColumn("s"), new ArrowStringDataFrameColumn("a"),
                new BooleanDataFrameColumn("b"), new SByteDataFrameColumn("i8"),
                new Int16DataFrameColumn("i16"), new Int32DataFrameColumn("i32"),
                new Int64DataFrameColumn("i64"), new SingleDataFrameColumn("f32"),
                new DoubleDataFrameColumn("f64"));
            using IEnumerator<PreparedArrowBatch> batches = DataFrameArrowBatchAdapter.Convert(frame).GetEnumerator();
            Assert.True(batches.MoveNext());
            RecordBatch batch = batches.Current.Batch;
            Assert.Equal(0, batch.Length);
            Assert.Equal(new[]
            {
                ArrowTypeId.String, ArrowTypeId.String, ArrowTypeId.Boolean, ArrowTypeId.Int8,
                ArrowTypeId.Int16, ArrowTypeId.Int32, ArrowTypeId.Int64, ArrowTypeId.Float, ArrowTypeId.Double
            }, batch.Schema.FieldsList.Select(field => field.DataType.TypeId));
            foreach (StringArray array in batch.Arrays.OfType<StringArray>())
            {
                Assert.Equal(new byte[4], array.ValueOffsetsBuffer.Memory.ToArray());
            }

            Assert.Empty(RoundTrip(batch));
            Assert.False(batches.MoveNext());
        }

        [Theory]
        [InlineData(65_535)]
        [InlineData(65_536)]
        [InlineData(65_537)]
        public void RowLimitPreservesEveryRowAcrossWindows(int rows)
        {
            var text = Enumerable.Range(0, rows).Select(i => "row-" + i).ToArray();
            var flags = Enumerable.Range(0, rows).Select(i => i % 5 == 0 ? (bool?)null : i % 3 != 0).ToArray();
            var frame = new DataFrame(
                new StringDataFrameColumn("s", text),
                new PrimitiveDataFrameColumn<int>("i", Enumerable.Range(0, rows)),
                new BooleanDataFrameColumn("b", flags));
            int processed = 0;
            int batchCount = 0;
            foreach (PreparedArrowBatch prepared in DataFrameArrowBatchAdapter.Convert(frame))
            {
                ++batchCount;
                Assert.InRange(prepared.Batch.Length, 1, DataFrameArrowBatchAdapter.MaxRowsPerBatch);
                foreach (object[] row in RoundTrip(prepared.Batch))
                {
                    Assert.Equal(text[processed], row[0]);
                    Assert.Equal(processed, row[1]);
                    Assert.Equal(flags[processed], row[2]);
                    ++processed;
                }
            }

            Assert.Equal(rows, processed);
            Assert.Equal(rows > DataFrameArrowBatchAdapter.MaxRowsPerBatch ? 2 : 1, batchCount);
        }

        [Fact]
        public void StringBudgetSplitsWindowsWithoutLosingNullEmptyOrArrowValues()
        {
            string[] text = { null, "", "你好", "a", "b", "", null, "c", "d", "e", "f", "g" };
            string[] arrow = { "first", "", null, "🙂", "", "", "", "h", "i", "j", "k", "last" };
            var frame = new DataFrame(new StringDataFrameColumn("s", text), MakeArrowColumn("a", arrow),
                new BooleanDataFrameColumn("b", Enumerable.Range(0, text.Length).Select(i => i % 3 != 0)),
                new Int64DataFrameColumn("n", Enumerable.Range(0, text.Length).Select(i => (long)i * 31)));
            var output = new List<object[]>();
            int batchCount = 0;
            foreach (PreparedArrowBatch prepared in DataFrameArrowBatchAdapter.Convert(frame, 9, 8, NewBuffer))
            {
                ++batchCount;
                Assert.InRange(prepared.Batch.Arrays.OfType<StringArray>().Sum(array => array.ValueBuffer.Length), 0, 8);
                Assert.InRange(prepared.Batch.Length, 1, 9);
                output.AddRange(RoundTrip(prepared.Batch));
            }

            Assert.True(batchCount > 1);
            AssertRows(ReadFrame(frame), output);
        }

        [Fact]
        public void AllNullAndEmptyValuesAdvanceWithoutPayloadBytes()
        {
            string[] strings = Enumerable.Range(0, 34).Select(i => i % 2 == 0 ? null : "").ToArray();
            var frame = new DataFrame(new StringDataFrameColumn("s", strings), MakeArrowColumn("a", strings));
            int processed = 0;
            foreach (PreparedArrowBatch prepared in DataFrameArrowBatchAdapter.Convert(frame, 9, 1, NewBuffer))
            {
                foreach (StringArray array in prepared.Batch.Arrays)
                {
                    Assert.Equal(0, array.ValueBuffer.Length);
                    Assert.Equal(new byte[(prepared.Batch.Length + 1) * 4], array.ValueOffsetsBuffer.Memory.ToArray());
                }

                foreach (object[] row in RoundTrip(prepared.Batch))
                {
                    Assert.Equal(strings[processed], row[0]);
                    Assert.Equal(strings[processed], row[1]);
                    ++processed;
                }
            }

            Assert.Equal(strings.Length, processed);
        }

        [Theory]
        [InlineData(7, 8)]
        [InlineData(8, 8)]
        [InlineData(9, 8)]
        [InlineData(8 * 1024 * 1024 - 1, 8 * 1024 * 1024)]
        [InlineData(8 * 1024 * 1024, 8 * 1024 * 1024)]
        [InlineData(8 * 1024 * 1024 + 1, 8 * 1024 * 1024)]
        public void StringByteLimitIsCheckedBeforeEncodingAllocations(int bytes, int limit)
        {
            var frame = new DataFrame(new StringDataFrameColumn("s", new[] { new string('x', bytes) }));
            int allocations = 0;
            IEnumerable<PreparedArrowBatch> batches = DataFrameArrowBatchAdapter.Convert(frame, 10, limit,
                length => { ++allocations; return new byte[length]; });
            if (bytes > limit)
            {
                InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => batches.First());
                Assert.Contains("Row 0", error.Message);
                Assert.Contains($"{bytes} UTF-8 string bytes", error.Message);
                Assert.Equal(0, allocations);
            }
            else
            {
                foreach (PreparedArrowBatch prepared in batches)
                {
                    Assert.Equal(new string('x', bytes), RoundTrip(prepared.Batch)[0][0]);
                    Assert.Equal(bytes, ((StringArray)prepared.Batch.Column(0)).ValueBuffer.Length);
                }
            }
        }

        [Fact]
        public void StringBudgetIncludesEveryColumnInOneRow()
        {
            var frame = new DataFrame(new StringDataFrameColumn("s", new[] { "你好" }),
                MakeArrowColumn("a", new[] { "🙂" }));
            int allocations = 0;
            InvalidOperationException error = Assert.Throws<InvalidOperationException>(() =>
                DataFrameArrowBatchAdapter.Convert(frame, 10, 9,
                    length => { ++allocations; return new byte[length]; }).First());
            Assert.Contains("10 UTF-8 string bytes", error.Message);
            Assert.Equal(0, allocations);
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(6)]
        public void BufferAllocationFailurePreservesInputAndOriginalError(int failAt)
        {
            var frame = new DataFrame(new StringDataFrameColumn("s", new[] { "hello", null }),
                MakeArrowColumn("a", new[] { "world", "🙂" }));
            object[][] before = ReadFrame(frame);
            var primary = new InvalidOperationException("injected-buffer-failure");
            int allocations = 0;
            Exception thrown = Assert.Throws<InvalidOperationException>(() =>
                DataFrameArrowBatchAdapter.Convert(frame, 4, 32,
                    length => ++allocations == failAt ? throw primary : new byte[length]).First());
            Assert.Same(primary, thrown);
            Assert.Equal(failAt, allocations);
            AssertRows(before, ReadFrame(frame));
            foreach (PreparedArrowBatch prepared in DataFrameArrowBatchAdapter.Convert(frame))
            {
                AssertRows(before, RoundTrip(prepared.Batch));
            }
        }

        [Fact]
        public void EarlyEnumeratorDisposalReleasesItsWindow()
        {
            var frame = new DataFrame(new StringDataFrameColumn("s", new[] { "one", "two" }));
            IEnumerator<PreparedArrowBatch> iterator = DataFrameArrowBatchAdapter.Convert(frame, 1, 32, NewBuffer).GetEnumerator();
            Assert.True(iterator.MoveNext());
            PreparedArrowBatch first = iterator.Current;
            Assert.Equal(1, first.OwnedRootCount);
            Assert.Equal(3, first.HeldReferenceCount);
            iterator.Dispose();
            Assert.Equal(0, first.OwnedRootCount);
            Assert.Equal(0, first.HeldReferenceCount);
            Assert.Equal(1, first.ReleasedRootCount);
            Assert.Equal(0, first.ReleaseFailedRootCount);
            Assert.Null(first.Batch);
            first.Dispose();
            Assert.Equal(1, first.ReleasedRootCount);
            Assert.Equal("one", frame.Columns[0][0]);
        }

        [Fact]
        public void MovingToTheNextWindowReleasesThePreviousLease()
        {
            var frame = new DataFrame(new StringDataFrameColumn("s", new[] { "one", "two" }));
            using IEnumerator<PreparedArrowBatch> iterator =
                DataFrameArrowBatchAdapter.Convert(frame, 1, 32, NewBuffer).GetEnumerator();
            Assert.True(iterator.MoveNext());
            PreparedArrowBatch first = iterator.Current;
            Assert.True(iterator.MoveNext());
            Assert.Null(first.Batch);
            Assert.Equal(0, first.HeldReferenceCount);
            Assert.Equal(1, first.ReleasedRootCount);
            Assert.Equal("two", ((StringArray)iterator.Current.Batch.Column(0)).GetString(0));
        }

        [Fact]
        public void UnsupportedMixedTypesAreRejectedBeforeAllocationOrCustomClone()
        {
            DataFrameColumn[] unsupported =
            {
                new ByteDataFrameColumn("x", new byte[] { 1 }),
                new UInt16DataFrameColumn("x", new ushort[] { 1 }),
                new UInt32DataFrameColumn("x", new uint[] { 1 }),
                new UInt64DataFrameColumn("x", new ulong[] { 1 }),
                new DateTimeDataFrameColumn("x", new[] { DateTime.UnixEpoch }),
                new CharDataFrameColumn("x", new[] { 'x' }),
                new DecimalDataFrameColumn("x", new[] { 1m }),
                new CustomIntColumn("x"),
                new CustomStringColumn("x")
            };
            foreach (DataFrameColumn column in unsupported)
            {
                var frame = new DataFrame(new StringDataFrameColumn("s", new[] { "value" }), column);
                int allocations = 0;
                NotSupportedException error = Assert.Throws<NotSupportedException>(() =>
                    DataFrameArrowBatchAdapter.Convert(frame, 8, 32,
                        length => { ++allocations; return new byte[length]; }).First());
                Assert.Contains(column.GetType().FullName, error.Message);
                Assert.Equal(0, allocations);
            }
        }

        [Fact]
        public void LegacyExportIsKeptWhenThereAreNoRegularStrings()
        {
            var frame = new DataFrame(new UInt32DataFrameColumn("legacy", new uint[] { 1, uint.MaxValue }));
            using IEnumerator<PreparedArrowBatch> batches = DataFrameArrowBatchAdapter.Convert(frame).GetEnumerator();
            Assert.True(batches.MoveNext());
            Assert.Equal(ArrowTypeId.UInt32, batches.Current.Batch.Schema.GetFieldByIndex(0).DataType.TypeId);
            Assert.Equal(uint.MaxValue, ((UInt32Array)batches.Current.Batch.Column(0)).GetValue(1));
            Assert.False(batches.MoveNext());
        }

        [Fact]
        public void NullAndColumnlessResultsFailDescriptively()
        {
            Assert.Contains("null DataFrame", Assert.Throws<ArgumentException>(() =>
                DataFrameArrowBatchAdapter.Convert(null).First()).Message);
            Assert.Contains("no columns", Assert.Throws<ArgumentException>(() =>
                DataFrameArrowBatchAdapter.Convert(new DataFrame()).First()).Message);
        }

        [Fact]
        public void RowMappingUsesLongPositionsAndCheckedAllocationArithmetic()
        {
            long start = (long)int.MaxValue + 123;
            Int64DataFrameColumn map = DataFrameArrowBatchAdapter.CreateRowMap(start, 3);
            Assert.Equal(new long?[] { start, start + 1, start + 2 }, map);
            Assert.Throws<OverflowException>(() => DataFrameArrowBatchAdapter.CreateRowMap(long.MaxValue, 1));
            Assert.Throws<OverflowException>(() => DataFrameArrowBatchAdapter.OffsetBufferLength(int.MaxValue));
            Assert.Throws<OverflowException>(() => DataFrameArrowBatchAdapter.OffsetBufferLength(int.MaxValue / 4));
        }

        [Fact]
        public void ArrowCursorCrossesPhysicalBuffersAndUsesLocalNullBits()
        {
            var values = new ReadOnlyMemory<byte>[]
            {
                Encoding.UTF8.GetBytes("a🙂"), ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes("二z")
            };
            var offsets = new ReadOnlyMemory<int>[]
            {
                new[] { 0, 1, 1, 5 }, new[] { 0 }, new[] { 0, 3, 4 }
            };
            var validity = new ReadOnlyMemory<byte>[] { new byte[] { 5 }, ReadOnlyMemory<byte>.Empty, new byte[] { 3 } };
            using var cursor = new DataFrameArrowBatchAdapter.ArrowStringCursor(values, offsets, validity, false);
            string[] expected = { "a", null, "🙂", "二", "z" };
            foreach (string text in expected)
            {
                ReadOnlyMemory<byte> bytes = cursor.Peek(out bool valid);
                Assert.Equal(text != null, valid);
                Assert.Equal(text == null ? System.Array.Empty<byte>() : Encoding.UTF8.GetBytes(text), bytes.ToArray());
                Assert.Equal(bytes.ToArray(), cursor.Peek(out bool _).ToArray());
                cursor.Advance();
            }
        }

        [Fact]
        public void MissingNullBitmapIsAllowedOnlyForAnAllValidArrowColumn()
        {
            var values = new ReadOnlyMemory<byte>[] { Encoding.UTF8.GetBytes("x") };
            var offsets = new ReadOnlyMemory<int>[] { new[] { 0, 1 } };
            var validity = new ReadOnlyMemory<byte>[] { ReadOnlyMemory<byte>.Empty };
            using var valid = new DataFrameArrowBatchAdapter.ArrowStringCursor(values, offsets, validity, true);
            Assert.Equal(new byte[] { (byte)'x' }, valid.Peek(out bool isValid).ToArray());
            Assert.True(isValid);
            using var absent = new DataFrameArrowBatchAdapter.ArrowStringCursor(
                values, offsets, System.Array.Empty<ReadOnlyMemory<byte>>(), true);
            Assert.Equal(new byte[] { (byte)'x' }, absent.Peek(out bool absentValid).ToArray());
            Assert.True(absentValid);
            using var invalid = new DataFrameArrowBatchAdapter.ArrowStringCursor(values, offsets, validity, false);
            Assert.Throws<InvalidOperationException>(() => invalid.Peek(out bool _));
        }

        private static byte[] NewBuffer(int length) => new byte[length];

        private static ArrowStringDataFrameColumn MakeArrowColumn(string name, string[] values)
        {
            using var stream = new MemoryStream();
            var offsets = new byte[4 * (values.Length + 1)];
            var valid = new byte[(values.Length + 7) / 8];
            int nullCount = 0;
            for (int i = 0; i < values.Length; ++i)
            {
                if (values[i] == null)
                {
                    ++nullCount;
                }
                else
                {
                    valid[i / 8] |= (byte)(1 << (i & 7));
                    byte[] value = Encoding.UTF8.GetBytes(values[i]);
                    stream.Write(value, 0, value.Length);
                }

                byte[] offset = BitConverter.GetBytes(checked((int)stream.Length));
                if (!BitConverter.IsLittleEndian)
                {
                    System.Array.Reverse(offset);
                }

                System.Array.Copy(offset, 0, offsets, (i + 1) * 4, 4);
            }

            return new ArrowStringDataFrameColumn(name, stream.ToArray(), offsets, valid, values.Length, nullCount);
        }

        private static object[][] ReadFrame(DataFrame frame) => Enumerable.Range(0, checked((int)frame.Rows.Count))
            .Select(row => frame.Columns.Select(column => column[row]).ToArray()).ToArray();

        private static void AssertRows(IEnumerable<object[]> expected, IEnumerable<object[]> actual)
        {
            object[][] expectedRows = expected.ToArray();
            object[][] actualRows = actual.ToArray();
            Assert.Equal(expectedRows.Length, actualRows.Length);
            for (int i = 0; i < expectedRows.Length; ++i)
            {
                Assert.Equal(expectedRows[i], actualRows[i]);
            }
        }

        private static List<object[]> RoundTrip(RecordBatch batch)
        {
            using var stream = new MemoryStream();
            using (var writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen: true))
            {
                writer.WriteRecordBatch(batch);
                writer.WriteEnd();
            }

            stream.Position = 0;
            using var reader = new ArrowStreamReader(stream);
            using RecordBatch read = reader.ReadNextRecordBatch();
            Assert.NotNull(read);
            Assert.Equal(batch.Length, read.Length);
            var rows = new List<object[]>();
            for (int row = 0; row < read.Length; ++row)
            {
                rows.Add(read.Arrays.Select(array => ReadValue(array, row)).ToArray());
            }

            Assert.Null(reader.ReadNextRecordBatch());
            return rows;
        }

        private static object ReadValue(IArrowArray array, int row)
        {
            if (array.IsNull(row)) { return null; }
            switch (array)
            {
                // Arrow 14 GetString treats a default empty value buffer as null,
                // so use the independent validity bitmap and UTF-8 bytes as the oracle.
                case StringArray value: return Encoding.UTF8.GetString(value.GetBytes(row));
                case BooleanArray value: return value.GetValue(row);
                case Int8Array value: return value.GetValue(row);
                case Int16Array value: return value.GetValue(row);
                case Int32Array value: return value.GetValue(row);
                case Int64Array value: return value.GetValue(row);
                case FloatArray value: return value.GetValue(row);
                case DoubleArray value: return value.GetValue(row);
                default: throw new InvalidOperationException($"Unexpected test array: {array.GetType()}");
            }
        }

        private sealed class CustomIntColumn : Int32DataFrameColumn
        {
            internal CustomIntColumn(string name) : base(name, new[] { 1 }) { }
        }

        private sealed class CustomStringColumn : StringDataFrameColumn
        {
            internal CustomStringColumn(string name) : base(name, new[] { "custom" }) { }
        }
    }
}
