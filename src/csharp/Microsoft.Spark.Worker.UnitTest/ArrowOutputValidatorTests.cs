// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Spark.Worker.Command;
using Xunit;
using SparkTypes = Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class ArrowOutputValidatorTests
    {
        [Fact]
        public void UsesPositionAndActualNullsInsteadOfFieldNamesOrNullableMetadata()
        {
            using RecordBatch batch = Batch(new Int32Array.Builder().Append(3).Build());
            ArrowOutputValidator.Validate(batch, Declared(new SparkTypes.IntegerType(), false));
            Assert.Equal("actual", batch.Schema.GetFieldByIndex(0).Name);
            Assert.True(batch.Schema.GetFieldByIndex(0).IsNullable);
        }

        [Fact]
        public void RejectsMissingExtraAndWrongLengthColumns()
        {
            using RecordBatch batch = Batch(new Int32Array.Builder().Append(3).Build());
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(batch,
                new SparkTypes.StructType(System.Array.Empty<SparkTypes.StructField>())));
            var twoFields = new SparkTypes.StructType(new[]
            {
                new SparkTypes.StructField("a", new SparkTypes.IntegerType()),
                new SparkTypes.StructField("b", new SparkTypes.IntegerType())
            });
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(batch, twoFields));
            using var wrongLength = new RecordBatch(batch.Schema, batch.Arrays, 0);
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(wrongLength,
                Declared(new SparkTypes.IntegerType())));
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(null, twoFields));
        }

        [Fact]
        public void RejectsNullAndTypeCoercion()
        {
            using RecordBatch nulls = Batch(new Int32Array.Builder().AppendNull().Build());
            ArrowOutputValidator.Validate(nulls, Declared(new SparkTypes.IntegerType()));
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(nulls,
                Declared(new SparkTypes.IntegerType(), false)));
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(nulls,
                Declared(new SparkTypes.LongType())));
            using RecordBatch unsigned = Batch(new UInt8Array.Builder().Append(255).Build());
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(unsigned,
                Declared(new SparkTypes.ByteType())));
        }

        [Fact]
        public void ValidatesPhysicalColumnTypeEvenIfResultSchemaClaimsCorrectType()
        {
            using var array = new Int64Array.Builder().Append(1).Build();
            var schema = new Schema.Builder().Field(new Field("a", Int32Type.Default, true)).Build();
            using var batch = new RecordBatch(schema, new IArrowArray[] { array }, 1);
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(batch,
                Declared(new SparkTypes.IntegerType())));
        }

        [Fact]
        public void ChecksNestedNullsOnlyUnderValidParentsIncludingSlices()
        {
            var child = new Int32Array.Builder().AppendNull().Append(7).AppendNull().Build();
            var type = new StructType(new[] { new Field("nested", Int32Type.Default, true) });
            var validity = new ArrowBuffer.BitmapBuilder().Append(false).Append(true).Append(true).Build();
            using var array = new StructArray(type, 3, new[] { child }, validity, 1);
            var expected = new SparkTypes.StructType(new[]
            {
                new SparkTypes.StructField("different", new SparkTypes.IntegerType(), false)
            });
            using RecordBatch firstTwo = Batch(array.Slice(0, 2));
            ArrowOutputValidator.Validate(firstTwo, Declared(expected));
            using RecordBatch second = Batch(array.Slice(1, 1));
            ArrowOutputValidator.Validate(second, Declared(expected));
            using RecordBatch third = Batch(array.Slice(2, 1));
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(third,
                Declared(expected)));
        }

        [Fact]
        public void ChecksListElementsAndSkipsNullLists()
        {
            var child = new Int32Array.Builder().AppendNull().Append(5).Build();
            var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(2).Build();
            var validity = new ArrowBuffer.BitmapBuilder().Append(false).Append(true).Build();
            using var array = new ListArray(new ListType(Int32Type.Default), 2,
                offsets, child, validity, 1);
            using RecordBatch batch = Batch(array);
            ArrowOutputValidator.Validate(batch,
                Declared(new SparkTypes.ArrayType(new SparkTypes.IntegerType(), false)));

            var builder = new ListArray.Builder(Int32Type.Default);
            builder.Append();
            ((Int32Array.Builder)builder.ValueBuilder).AppendNull();
            using RecordBatch invalid = Batch(builder.Build());
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(invalid,
                Declared(new SparkTypes.ArrayType(new SparkTypes.IntegerType(), false))));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void ChecksMapKeyAndValueNulls(bool nullKey)
        {
            var builder = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default, false));
            builder.Append();
            var keys = (StringArray.Builder)builder.KeyBuilder;
            var values = (Int32Array.Builder)builder.ValueBuilder;
            if (nullKey)
            {
                keys.AppendNull();
                values.Append(1);
            }
            else
            {
                keys.Append("a");
                values.AppendNull();
            }

            using RecordBatch batch = Batch(builder.Build());
            var declared = new SparkTypes.MapType(new SparkTypes.StringType(),
                new SparkTypes.IntegerType(), false);
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(batch, Declared(declared)));
            if (!nullKey)
            {
                ArrowOutputValidator.Validate(batch, Declared(new SparkTypes.MapType(
                    new SparkTypes.StringType(), new SparkTypes.IntegerType(), true)));
            }
        }

        [Theory]
        [InlineData("UTC")]
        [InlineData("America/Los_Angeles")]
        [InlineData("+08:00")]
        public void AcceptsMicrosecondTimestampWithoutConvertingEpoch(string timezone)
        {
            var type = new TimestampType(TimeUnit.Microsecond, timezone);
            using RecordBatch batch = Batch(new TimestampArray.Builder(type)
                .Append(DateTimeOffset.UnixEpoch.AddSeconds(1)).Build());
            ArrowOutputValidator.Validate(batch, Declared(new SparkTypes.TimestampType()));
            Assert.Equal(1000000L, ((TimestampArray)batch.Column(0)).Values[0]);
        }

        [Theory]
        [InlineData(TimeUnit.Millisecond, "UTC")]
        [InlineData(TimeUnit.Microsecond, "")]
        public void RejectsWrongTimestampUnitOrMissingTimezone(TimeUnit unit, string timezone)
        {
            using RecordBatch batch = Batch(new TimestampArray.Builder(unit, timezone)
                .Append(DateTimeOffset.UnixEpoch).Build());
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(batch,
                Declared(new SparkTypes.TimestampType())));
        }

        [Fact]
        public void ValidatesDecimalPrecisionAndScale()
        {
            using RecordBatch batch = Batch(new Decimal128Array.Builder(new Decimal128Type(8, 2))
                .Append(12.34m).Build());
            ArrowOutputValidator.Validate(batch, Declared(new SparkTypes.DecimalType(8, 2)));
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(batch,
                Declared(new SparkTypes.DecimalType(9, 2))));
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.Validate(batch,
                Declared(new SparkTypes.DecimalType(8, 3))));
        }

        [Fact]
        public void RequiresStablePhysicalSchemaAcrossBatches()
        {
            Schema first = Schema(new TimestampType(TimeUnit.Microsecond, "UTC"));
            ArrowOutputValidator.ValidateSchema(first, Schema(new TimestampType(TimeUnit.Microsecond, "UTC")));
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.ValidateSchema(first,
                Schema(new TimestampType(TimeUnit.Millisecond, "UTC"))));
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.ValidateSchema(first,
                Schema(new TimestampType(TimeUnit.Microsecond, "Asia/Shanghai"))));
            Assert.Throws<InvalidDataException>(() => ArrowOutputValidator.ValidateSchema(first,
                Schema(new TimestampType(TimeUnit.Microsecond, "UTC"), "renamed")));
        }

        private static Schema Schema(IArrowType type, string name = "actual") =>
            new Schema.Builder().Field(new Field(name, type, true)).Build();

        private static RecordBatch Batch(IArrowArray array) =>
            new RecordBatch(Schema(array.Data.DataType), new[] { array }, array.Length);

        private static SparkTypes.StructType Declared(SparkTypes.DataType type, bool nullable = true) =>
            new SparkTypes.StructType(new[] { new SparkTypes.StructField("declared", type, nullable) });
    }
}
