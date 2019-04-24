// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

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
    /// Represents a date type.
    /// </summary>
    public sealed class DateType : AtomicType
    {
    }

    /// <summary>
    /// Represents a timestamp type.
    /// </summary>
    public sealed class TimestampType : AtomicType
    {
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
    /// Represents a decimal type (not implemented).
    /// </summary>
    public sealed class DecimalType : FractionalType
    {
        /// <summary>
        /// Initializes the <see cref="DecimalType"/> instance.
        /// </summary>
        public DecimalType()
        {
            throw new NotImplementedException();
        }
    }
}
