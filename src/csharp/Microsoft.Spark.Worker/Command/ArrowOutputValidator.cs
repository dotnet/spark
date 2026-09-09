// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Types;
using SparkTypes = Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Worker.Command
{
    /// <summary>
    /// Checks Spark 4 grouped-map results before they enter the Arrow output stream.
    /// Spark's grouped-map reader does not check the complete declared return schema.
    /// </summary>
    internal static class ArrowOutputValidator
    {
        internal static void Validate(RecordBatch batch, SparkTypes.StructType schema)
        {
            if (batch == null || schema == null || batch.Length < 0 ||
                batch.ColumnCount != schema.Fields.Count ||
                batch.Schema.FieldsList.Count != schema.Fields.Count)
            {
                throw new InvalidDataException("Grouped-map result must match the declared column count.");
            }

            for (int i = 0; i < schema.Fields.Count; ++i)
            {
                IArrowArray column = batch.Column(i);
                if (column == null || column.Length != batch.Length)
                {
                    throw new InvalidDataException("Grouped-map result columns must match the result row count.");
                }

                // The public Apply API is positional, including nested structs.
                SparkTypes.StructField field = schema.Fields[i];
                ValidateType(batch.Schema.GetFieldByIndex(i).DataType, field.DataType);
                ValidateLayout(column, batch.Schema.GetFieldByIndex(i).DataType);
                ValidateNulls(column, field.DataType, field.IsNullable, 0, batch.Length);
            }
        }

        internal static void ValidateSchema(Schema expected, Schema actual)
        {
            ValidateFields(expected.FieldsList, actual.FieldsList);
        }

        private static void ValidateFields(IReadOnlyList<Field> expected, IReadOnlyList<Field> actual)
        {
            if (expected.Count != actual.Count)
            {
                throw new InvalidDataException("Arrow result schema changed between batches.");
            }

            for (int i = 0; i < expected.Count; ++i)
            {
                if (expected[i].Name != actual[i].Name ||
                    expected[i].IsNullable != actual[i].IsNullable)
                {
                    throw new InvalidDataException("Arrow result schema changed between batches.");
                }

                ValidateSameType(expected[i].DataType, actual[i].DataType);
            }
        }

        private static void ValidateSameType(IArrowType expected, IArrowType actual)
        {
            if (expected.TypeId != actual.TypeId || expected.GetType() != actual.GetType())
            {
                throw new InvalidDataException("Arrow result has an inconsistent physical type.");
            }

            if (expected is Decimal128Type decimalType &&
                (decimalType.Precision != ((Decimal128Type)actual).Precision ||
                 decimalType.Scale != ((Decimal128Type)actual).Scale))
            {
                throw new InvalidDataException("Arrow decimal precision or scale changed.");
            }

            if (expected is TimestampType timestamp &&
                (timestamp.Unit != ((TimestampType)actual).Unit ||
                 timestamp.Timezone != ((TimestampType)actual).Timezone))
            {
                throw new InvalidDataException("Arrow timestamp unit or timezone changed.");
            }

            if (expected is NestedType nested)
            {
                ValidateFields(nested.Fields, ((NestedType)actual).Fields);
            }
        }

        private static void ValidateType(IArrowType actual, SparkTypes.DataType expected)
        {
            bool matches = expected switch
            {
                SparkTypes.BooleanType _ => actual.TypeId == ArrowTypeId.Boolean,
                SparkTypes.ByteType _ => actual.TypeId == ArrowTypeId.Int8,
                SparkTypes.ShortType _ => actual.TypeId == ArrowTypeId.Int16,
                SparkTypes.IntegerType _ => actual.TypeId == ArrowTypeId.Int32,
                SparkTypes.LongType _ => actual.TypeId == ArrowTypeId.Int64,
                SparkTypes.FloatType _ => actual.TypeId == ArrowTypeId.Float,
                SparkTypes.DoubleType _ => actual.TypeId == ArrowTypeId.Double,
                SparkTypes.StringType _ => actual.TypeId == ArrowTypeId.String,
                SparkTypes.BinaryType _ => actual.TypeId == ArrowTypeId.Binary,
                SparkTypes.DateType _ => actual.TypeId == ArrowTypeId.Date32,
                SparkTypes.NullType _ => actual.TypeId == ArrowTypeId.Null,
                SparkTypes.DecimalType dec => actual is Decimal128Type arrowDecimal &&
                    dec.SimpleString == new SparkTypes.DecimalType(
                        arrowDecimal.Precision, arrowDecimal.Scale).SimpleString,
                SparkTypes.TimestampType _ => actual is TimestampType timestamp &&
                    timestamp.Unit == TimeUnit.Microsecond &&
                    !string.IsNullOrEmpty(timestamp.Timezone),
                SparkTypes.ArrayType _ => actual is ListType,
                SparkTypes.MapType _ => actual is MapType,
                SparkTypes.StructType _ => actual is StructType,
                _ => false
            };

            if (!matches)
            {
                throw new InvalidDataException("Grouped-map result type does not match the declared Spark type.");
            }

            if (expected is SparkTypes.ArrayType list)
            {
                ValidateType(((ListType)actual).ValueDataType, list.ElementType);
            }
            else if (expected is SparkTypes.MapType map)
            {
                var arrowMap = (MapType)actual;
                ValidateType(arrowMap.KeyField.DataType, map.KeyType);
                ValidateType(arrowMap.ValueField.DataType, map.ValueType);
            }
            else if (expected is SparkTypes.StructType structure)
            {
                IReadOnlyList<Field> fields = ((StructType)actual).Fields;
                if (fields.Count != structure.Fields.Count)
                {
                    throw new InvalidDataException("Grouped-map struct has an unexpected number of fields.");
                }

                for (int i = 0; i < fields.Count; ++i)
                {
                    ValidateType(fields[i].DataType, structure.Fields[i].DataType);
                }
            }
        }

        private static void ValidateLayout(IArrowArray array, IArrowType fieldType)
        {
            if (array == null || array.Length < 0 || array.Offset < 0)
            {
                throw new InvalidDataException("Invalid Arrow result array.");
            }

            ValidateSameType(fieldType, array.Data.DataType);
            if (array is StructArray structure)
            {
                IReadOnlyList<Field> fields = ((StructType)fieldType).Fields;
                if (fields.Count != structure.Fields.Count)
                {
                    throw new InvalidDataException("Arrow struct array does not match its schema.");
                }

                for (int i = 0; i < fields.Count; ++i)
                {
                    CheckRange(structure.Fields[i], structure.Offset, structure.Length);
                    ValidateLayout(structure.Fields[i], fields[i].DataType);
                }
            }
            else if (array is MapArray map)
            {
                ValidateLayout(map.KeyValues, ((MapType)fieldType).KeyValueType);
            }
            else if (array is ListArray list)
            {
                ValidateLayout(list.Values, ((ListType)fieldType).ValueDataType);
            }
        }

        private static void CheckRange(IArrowArray array, int start, int count)
        {
            if (array == null || start < 0 || count < 0 || start > array.Length - count)
            {
                throw new InvalidDataException("Arrow result contains an invalid child range.");
            }
        }

        private static void ValidateNulls(
            IArrowArray array,
            SparkTypes.DataType type,
            bool nullable,
            int start,
            int count)
        {
            CheckRange(array, start, count);
            for (int i = start; i < start + count; ++i)
            {
                if (array.IsNull(i))
                {
                    if (!nullable)
                    {
                        throw new InvalidDataException("Grouped-map result contains null in a non-nullable field.");
                    }

                    // Child validity is irrelevant beneath a null parent.
                    continue;
                }

                if (type is SparkTypes.StructType structure)
                {
                    var values = (StructArray)array;
                    for (int fieldIndex = 0; fieldIndex < structure.Fields.Count; ++fieldIndex)
                    {
                        SparkTypes.StructField field = structure.Fields[fieldIndex];
                        ValidateNulls(values.Fields[fieldIndex], field.DataType,
                            field.IsNullable, values.Offset + i, 1);
                    }
                }
                else if (type is SparkTypes.MapType mapType)
                {
                    var map = (MapArray)array;
                    int first = map.ValueOffsets[i];
                    int length = map.GetValueLength(i);
                    CheckRange(map.KeyValues, first, length);
                    for (int entry = first; entry < first + length; ++entry)
                    {
                        if (map.KeyValues.IsNull(entry))
                        {
                            throw new InvalidDataException("Arrow map entries cannot be null.");
                        }
                    }

                    ValidateNulls(map.Keys, mapType.KeyType, false,
                        map.KeyValues.Offset + first, length);
                    ValidateNulls(map.Values, mapType.ValueType, mapType.ValueContainsNull,
                        map.KeyValues.Offset + first, length);
                }
                else if (type is SparkTypes.ArrayType listType)
                {
                    var list = (ListArray)array;
                    ValidateNulls(list.Values, listType.ElementType, listType.ContainsNull,
                        list.ValueOffsets[i], list.GetValueLength(i));
                }
            }
        }
    }
}
