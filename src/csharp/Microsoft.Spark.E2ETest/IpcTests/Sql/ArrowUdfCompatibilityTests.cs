// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using Column = Microsoft.Spark.Sql.Column;
using DataFrame = Microsoft.Spark.Sql.DataFrame;
using FxDataFrame = Microsoft.Data.Analysis.DataFrame;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    [Trait("Category", "ArrowUdf")]
    public class ArrowUdfCompatibilityTests
    {
        private readonly SparkSession _spark;

        public ArrowUdfCompatibilityTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_0_0, Versions.V4_1_0)]
        public void ScalarUdfsPreserveNullsAcrossBatchesAndChains()
        {
            WithSqlConfig("spark.sql.execution.arrow.maxRecordsPerBatch", "2", () =>
            {
                foreach (bool useDataFrame in new[] { false, true })
                {
                    DataFrame input = _spark.Range(0, 7, 1, 1).SelectExpr(
                        "id", "case when id % 2 = 0 then cast(null as string) " +
                        "else concat('row-', cast(id as string)) end as label");
                    Func<Column, Column> increment = useDataFrame ?
                        DataFrameFunctions.VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>(
                            values => IncrementBatch(values)) :
                        ArrowFunctions.VectorUdf<Int64Array, Int64Array>(
                            values => IncrementBatch(values));
                    Func<Column, Column> identity = useDataFrame ?
                        DataFrameFunctions.VectorUdf<ArrowStringDataFrameColumn,
                            ArrowStringDataFrameColumn>(values => values) :
                        ArrowFunctions.VectorUdf<StringArray, StringArray>(values => values);

                    Row[] rows = input.Select(input["id"],
                        increment(increment(input["id"])), identity(input["label"]))
                        .Collect().ToArray();

                    Assert.Equal(7, rows.Length);
                    for (int i = 0; i < rows.Length; ++i)
                    {
                        Assert.Equal((long)i, rows[i].GetAs<long>(0));
                        Assert.Equal(i + 2L, rows[i].GetAs<long>(1));
                        Assert.Equal(i % 2 == 0 ? null : $"row-{i}",
                            rows[i].GetAs<string>(2));
                    }
                }
            });
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V3_0_0, Versions.V4_1_0)]
        public void ScalarUdfsAcceptTenArguments()
        {
            DataFrame input = _spark.Range(0, 3, 1, 1).SelectExpr(
                Enumerable.Range(0, 10).Select(i => $"id + {i} as c{i}").ToArray());
            Column[] columns = input.Columns().Select(name => input[name]).ToArray();
            var arrow = ArrowFunctions.VectorUdf<Int64Array, Int64Array, Int64Array,
                Int64Array, Int64Array, Int64Array, Int64Array, Int64Array,
                Int64Array, Int64Array, Int64Array>(
                (a, b, c, d, e, f, g, h, i, j) => SumColumns(a, b, c, d, e, f, g, h, i, j));
            var dataFrame = DataFrameFunctions.VectorUdf<Int64DataFrameColumn,
                Int64DataFrameColumn, Int64DataFrameColumn, Int64DataFrameColumn,
                Int64DataFrameColumn, Int64DataFrameColumn, Int64DataFrameColumn,
                Int64DataFrameColumn, Int64DataFrameColumn, Int64DataFrameColumn,
                Int64DataFrameColumn>(
                (a, b, c, d, e, f, g, h, i, j) => SumColumns(a, b, c, d, e, f, g, h, i, j));

            // Keep the representations in separate tasks: mixing them is unsupported.
            foreach (Column result in new[]
            {
                arrow(columns[0], columns[1], columns[2], columns[3], columns[4],
                    columns[5], columns[6], columns[7], columns[8], columns[9]),
                dataFrame(columns[0], columns[1], columns[2], columns[3], columns[4],
                    columns[5], columns[6], columns[7], columns[8], columns[9])
            })
            {
                Assert.Equal(new long[] { 45, 55, 65 }, input.Select(result).Collect()
                    .Select(row => row.GetAs<long>(0)).ToArray());
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void EmptyInputDoesNotInvokeScalarOrGroupedUdfs()
        {
            DataFrame empty = _spark.Range(0, 0, 1, 2);
            var arrow = ArrowFunctions.VectorUdf<Int64Array, Int64Array>(values =>
                throw new InvalidOperationException("An empty input must not invoke the UDF."));
            var dataFrame = DataFrameFunctions.VectorUdf<Int64DataFrameColumn,
                Int64DataFrameColumn>(values =>
                throw new InvalidOperationException("An empty input must not invoke the UDF."));
            Assert.Empty(empty.Select(arrow(empty["id"])).Collect());
            Assert.Empty(empty.Select(dataFrame(empty["id"])).Collect());
            Assert.Empty(empty.GroupBy("id").Apply(empty.Schema(),
                (RecordBatch batch) => throw new InvalidOperationException(
                    "An empty input must not create a synthetic group.")).Collect());
            Assert.Empty(empty.GroupBy("id").Apply(empty.Schema(),
                (FxDataFrame batch) => throw new InvalidOperationException(
                    "An empty input must not create a synthetic group.")).Collect());
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void GroupedUdfsProjectOriginalColumnsForOverlappingAndComputedKeys()
        {
            DataFrame input = _spark.Range(0, 6, 1, 1).SelectExpr(
                new[] { "cast(id % 2 as int) as c0" }.Concat(
                    Enumerable.Range(1, 11).Select(i => $"cast(id + {i} as int) as c{i}"))
                    .ToArray());
            Row[] expected = input.OrderBy("c1").Collect().ToArray();

            foreach (bool useDataFrame in new[] { false, true })
            {
                foreach (Column key in new[] { input["c0"], Functions.Expr("c1 % 3") })
                {
                    RelationalGroupedDataset grouped = input.GroupBy(key);
                    DataFrame result = useDataFrame ?
                        grouped.Apply(input.Schema(), (FxDataFrame batch) => CheckWideGroup(batch)) :
                        grouped.Apply(input.Schema(), (RecordBatch batch) => CheckWideGroup(batch));
                    Row[] actual = result.OrderBy("c1").Collect().ToArray();
                    Assert.Equal(expected.Length, actual.Length);
                    for (int row = 0; row < actual.Length; ++row)
                    {
                        for (int column = 0; column < 12; ++column)
                        {
                            Assert.Equal(expected[row].GetAs<int>(column),
                                actual[row].GetAs<int>(column));
                        }
                    }
                }
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void GroupedUdfsAllowZeroRowsAndNoGroupingKeys()
        {
            DataFrame input = _spark.Range(0, 3, 1, 1).SelectExpr("cast(id as int) as value");
            foreach (RelationalGroupedDataset grouped in new[]
            {
                input.GroupBy("value"), input.GroupBy()
            })
            {
                Assert.Empty(grouped.Apply(input.Schema(), (RecordBatch batch) =>
                    new RecordBatch(batch.Schema,
                        new IArrowArray[] { new Int32Array.Builder().Build() }, 0)).Collect());
                Assert.Empty(grouped.Apply(input.Schema(), (FxDataFrame batch) =>
                    new FxDataFrame(new Int32DataFrameColumn("value"))).Collect());
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void ZeroColumnGroupedInputPreservesRowCount()
        {
            RelationalGroupedDataset grouped = _spark.Range(0, 3, 1, 1)
                .Select(System.Array.Empty<Column>()).GroupBy();
            var schema = new StructType(new[]
            {
                new StructField("count", new LongType(), false)
            });

            DataFrame arrow = grouped.Apply(schema, (RecordBatch batch) =>
            {
                if (batch.ColumnCount != 0)
                {
                    throw new InvalidOperationException("Expected a zero-column Arrow group.");
                }

                return new RecordBatch(new Schema(new[]
                {
                    new Field("count", Apache.Arrow.Types.Int64Type.Default, false)
                }, null), new IArrowArray[]
                {
                    new Int64Array.Builder().Append(batch.Length).Build()
                }, 1);
            });
            DataFrame dataFrame = grouped.Apply(schema, (FxDataFrame batch) =>
            {
                if (batch.Columns.Count != 0)
                {
                    throw new InvalidOperationException("Expected a zero-column DataFrame group.");
                }

                return new FxDataFrame(new Int64DataFrameColumn(
                    "count", new long[] { batch.Rows.Count }));
            });

            Assert.Equal(3L, Assert.Single(arrow.Collect()).GetAs<long>(0));
            Assert.Equal(3L, Assert.Single(dataFrame.Collect()).GetAs<long>(0));
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void GroupedArrowTimestampPreservesEpochInSessionTimeZones()
        {
            foreach (string timeZone in new[] { "UTC", "America/Los_Angeles" })
            {
                WithSqlConfig("spark.sql.session.timeZone", timeZone, () =>
                {
                    DataFrame input = _spark.Range(0, 3, 1, 1).SelectExpr(
                        "id", "timestamp_seconds(1700000000 + id) as eventTime");
                    DataFrame result = input.GroupBy("id").Apply(input.Schema(),
                        (RecordBatch batch) => batch);
                    Row[] rows = result.OrderBy("id").SelectExpr(
                        "id", "unix_micros(eventTime) as micros").Collect().ToArray();
                    Assert.Equal(3, rows.Length);
                    for (int i = 0; i < rows.Length; ++i)
                    {
                        Assert.Equal((long)i, rows[i].GetAs<long>(0));
                        Assert.Equal((1700000000L + i) * 1000000, rows[i].GetAs<long>(1));
                    }
                });
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void GroupedUdfsRejectMismatchedSchemasAndAllowNextTask()
        {
            var schema = new StructType(new[]
            {
                new StructField("a", new IntegerType(), false),
                new StructField("b", new IntegerType())
            });
            foreach (bool useDataFrame in new[] { false, true })
            {
                foreach (string mismatch in new[] { "missing", "extra", "type", "null" })
                {
                    RelationalGroupedDataset grouped = _spark.Range(1).GroupBy("id");
                    DataFrame result = useDataFrame ?
                        grouped.Apply(schema, (FxDataFrame batch) => InvalidDataFrame(mismatch)) :
                        grouped.Apply(schema, (RecordBatch batch) => InvalidArrowBatch(mismatch));
                    Exception error = Assert.ThrowsAny<Exception>(() => result.Collect().ToArray());
                    Assert.Contains("InvalidDataException", error.ToString());
                    AssertHealthyScalarTask(useDataFrame);
                }
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void ScalarUdfsRejectWrongResultLengthAndAllowNextTask()
        {
            foreach (bool useDataFrame in new[] { false, true })
            {
                DataFrame input = _spark.Range(0, 2, 1, 1);
                Func<Column, Column> shorten = useDataFrame ?
                    DataFrameFunctions.VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>(
                        values => new Int64DataFrameColumn("value")) :
                    ArrowFunctions.VectorUdf<Int64Array, Int64Array>(
                        values => new Int64Array.Builder().Build());
                Exception error = Assert.ThrowsAny<Exception>(() =>
                    input.Select(shorten(input["id"])).Collect().ToArray());
                Assert.Contains("InvalidDataException", error.ToString());
                AssertHealthyScalarTask(useDataFrame);
            }
        }

        [SkipIfSparkVersionIsNotInRange(Versions.V4_0_0, Versions.V4_1_0)]
        public void ScalarFailuresBeforeAndAfterFirstBatchAllowNextTask()
        {
            WithSqlConfig("spark.sql.execution.arrow.maxRecordsPerBatch", "2", () =>
            {
                foreach (bool useDataFrame in new[] { false, true })
                {
                    foreach (long failingValue in new long[] { 0, 2 })
                    {
                        DataFrame input = _spark.Range(0, 6, 1, 1);
                        Func<Column, Column> fail = useDataFrame ?
                            DataFrameFunctions.VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>(
                                values => FailAtBatch(values, failingValue)) :
                            ArrowFunctions.VectorUdf<Int64Array, Int64Array>(
                                values => FailAtBatch(values, failingValue));
                        Exception error = Assert.ThrowsAny<Exception>(() =>
                            input.Select(fail(input["id"])).Collect().ToArray());
                        Assert.Contains("Arrow batch delegate failure.", error.ToString());
                        AssertHealthyScalarTask(useDataFrame);
                    }
                }
            });
        }

        private void AssertHealthyScalarTask(bool useDataFrame)
        {
            DataFrame input = _spark.Range(0, 3, 1, 1);
            Func<Column, Column> identity = useDataFrame ?
                DataFrameFunctions.VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>(
                    values => values) :
                ArrowFunctions.VectorUdf<Int64Array, Int64Array>(values => values);
            Assert.Equal(new long[] { 0, 1, 2 }, input.Select(identity(input["id"]))
                .Collect().Select(row => row.GetAs<long>(0)).ToArray());
        }

        private void WithSqlConfig(string key, string value, Action action)
        {
            RuntimeConfig config = _spark.Conf();
            string previous = config.Get(key);
            try
            {
                config.Set(key, value);
                action();
            }
            finally
            {
                config.Set(key, previous);
            }
        }

        private static Int64Array IncrementBatch(Int64Array values)
        {
            CheckBatchLength(values.Length);
            var output = new Int64Array.Builder();
            for (int i = 0; i < values.Length; ++i)
            {
                output.Append(values.GetValue(i).Value + 1);
            }

            return output.Build();
        }

        private static Int64DataFrameColumn IncrementBatch(Int64DataFrameColumn values)
        {
            CheckBatchLength(values.Length);
            var output = new Int64DataFrameColumn("value");
            for (long i = 0; i < values.Length; ++i)
            {
                output.Append(values[i] + 1);
            }

            return output;
        }

        private static void CheckBatchLength(long length)
        {
            if (length > 2)
            {
                throw new InvalidOperationException("The configured Arrow batch limit was not used.");
            }
        }

        private static Int64Array SumColumns(params Int64Array[] columns)
        {
            var output = new Int64Array.Builder();
            for (int i = 0; i < columns[0].Length; ++i)
            {
                output.Append(columns.Sum(column => column.GetValue(i).Value));
            }

            return output.Build();
        }

        private static Int64DataFrameColumn SumColumns(params Int64DataFrameColumn[] columns)
        {
            var output = new Int64DataFrameColumn("sum");
            for (long i = 0; i < columns[0].Length; ++i)
            {
                output.Append(columns.Sum(column => column[i].Value));
            }

            return output;
        }

        private static RecordBatch CheckWideGroup(RecordBatch batch)
        {
            if (batch.ColumnCount != 12 || batch.Schema.GetFieldByIndex(0).Name != "c0" ||
                batch.Schema.GetFieldByIndex(11).Name != "c11")
            {
                throw new InvalidOperationException("The grouped UDF did not receive original columns.");
            }

            return batch;
        }

        private static FxDataFrame CheckWideGroup(FxDataFrame batch)
        {
            if (batch.Columns.Count != 12 || batch.Columns[0].Name != "c0" ||
                batch.Columns[11].Name != "c11")
            {
                throw new InvalidOperationException("The grouped UDF did not receive original columns.");
            }

            return batch;
        }

        private static RecordBatch InvalidArrowBatch(string mismatch)
        {
            int count = mismatch == "missing" ? 1 : mismatch == "extra" ? 3 : 2;
            var fields = new Field[count];
            var columns = new IArrowArray[count];
            for (int i = 0; i < count; ++i)
            {
                columns[i] = i == 0 && mismatch == "type" ?
                    (IArrowArray)new Int64Array.Builder().Append(1).Build() :
                    i == 0 && mismatch == "null" ?
                    new Int32Array.Builder().AppendNull().Build() :
                    new Int32Array.Builder().Append(1).Build();
                fields[i] = new Field($"c{i}", columns[i].Data.DataType, true);
            }

            return new RecordBatch(new Schema(fields, null), columns, 1);
        }

        private static FxDataFrame InvalidDataFrame(string mismatch)
        {
            int count = mismatch == "missing" ? 1 : mismatch == "extra" ? 3 : 2;
            var columns = new DataFrameColumn[count];
            for (int i = 0; i < count; ++i)
            {
                columns[i] = i == 0 && mismatch == "type" ?
                    (DataFrameColumn)new Int64DataFrameColumn($"c{i}", new long[] { 1 }) :
                    new Int32DataFrameColumn($"c{i}",
                        new int?[] { i == 0 && mismatch == "null" ? null : (int?)1 });
            }

            return new FxDataFrame(columns);
        }

        private static Int64Array FailAtBatch(Int64Array values, long failingValue)
        {
            if (values.Length != 0 && values.GetValue(0) == failingValue)
            {
                throw new InvalidOperationException("Arrow batch delegate failure.");
            }

            return values;
        }

        private static Int64DataFrameColumn FailAtBatch(
            Int64DataFrameColumn values, long failingValue)
        {
            if (values.Length != 0 && values[0] == failingValue)
            {
                throw new InvalidOperationException("Arrow batch delegate failure.");
            }

            return values;
        }
    }
}
