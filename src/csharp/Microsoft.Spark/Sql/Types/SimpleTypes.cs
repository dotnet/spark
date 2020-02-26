// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Text.RegularExpressions;

namespace Microsoft.Spark.Sql.Types
{
    /// <summary>
    /// An internal type used to represent everything that is not null, arrays, structs, and maps.
    /// </summary>
    public abstract class AtomicType : DataType
    {
    }

    /// <summary>
    /// Represents a numeric type.
    /// </summary>
    public abstract class NumericType : AtomicType
    {
    }

    /// <summary>
    /// Represents an integral type.
    /// </summary>
    public abstract class IntegralType : NumericType
    {
    }

    /// <summary>
    /// Represents a fractional type.
    /// </summary>
    public abstract class FractionalType : NumericType
    {
    }

    /// <summary>
    /// Represents a null type.
    /// </summary>
    public sealed class NullType : DataType
    {
    }

    /// <summary>
    /// Represents a string type.
    /// </summary>
    public sealed class StringType : AtomicType
    {
    }

    /// <summary>
    /// Represents a binary (byte array) type.
    /// </summary>
    public sealed class BinaryType : AtomicType
    {
    }

    /// <summary>
    /// Represents a boolean type.
    /// </summary>
    public sealed class BooleanType : AtomicType
    {
    }

    /// <summary>
    /// Represents a date type. It represents a valid date in the proleptic Gregorian
    /// calendar. Valid range is [0001-01-01, 9999-12-31].
    /// </summary>
    public sealed class DateType : AtomicType
    {
        private static readonly DateTime s_unixTimeEpoch = new DateTime(1970, 1, 1);

        internal override bool NeedConversion() => true;

        internal override object FromInternal(object obj)
        {
            return new Date(new DateTime((int)obj * TimeSpan.TicksPerDay + s_unixTimeEpoch.Ticks));
        }
    }

    /// <summary>
    /// Represents a timestamp type. It represents a time instant in microsecond precision.
    /// Valid range is [0001-01-01T00:00:00.000000Z, 9999-12-31T23:59:59.999999Z] where
    /// the left/right-bound is a date and time of the proleptic Gregorian calendar in UTC+00:00.
    /// </summary>
    public sealed class TimestampType : AtomicType
    {
        private static readonly DateTime s_unixTimeEpoch =
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        internal override bool NeedConversion() => true;

        internal override object FromInternal(object obj)
        {
            return new Timestamp(new DateTime((int)obj * 10 + s_unixTimeEpoch.Ticks, DateTimeKind.Utc));
        }
    }

    /// <summary>
    /// Represents a double type.
    /// </summary>
    public sealed class DoubleType : FractionalType
    {
    }

    /// <summary>
    /// Represents a float type.
    /// </summary>
    public sealed class FloatType : FractionalType
    {
    }

    /// <summary>
    /// Represents a byte type.
    /// </summary>
    public sealed class ByteType : IntegralType
    {
    }

    /// <summary>
    /// Represents an int type.
    /// </summary>
    public sealed class IntegerType : IntegralType
    {
    }

    /// <summary>
    /// Represents a long type.
    /// </summary>
    public sealed class LongType : IntegralType
    {
    }

    /// <summary>
    /// Represents a short type.
    /// </summary>
    public sealed class ShortType : IntegralType
    {
    }

    /// <summary>
    /// Represents a decimal type.
    /// </summary>
    public sealed class DecimalType : FractionalType
    {
        internal static Regex s_fixedDecimal =
            new Regex(@"decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)", RegexOptions.Compiled);

        private readonly int _precision;
        private readonly int _scale;

        /// <summary>
        /// Initializes the <see cref="DecimalType"/> instance.
        /// </summary>
        /// <remarks>
        /// Default values of precision and scale are from Scala:
        /// sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala.
        /// </remarks>
        /// <param name="precision">Number of digits in a number</param>
        /// <param name="scale">
        /// Number of digits to the right of the decimal point in a number
        /// </param>
        public DecimalType(int precision = 10, int scale = 0)
        {
            _precision = precision;
            _scale = scale;
        }

        /// <summary>
        /// Returns simple string version of DecimalType.
        /// </summary>
        public override string SimpleString => $"decimal({_precision},{_scale})";

        internal override object JsonValue => SimpleString;
    }
}
