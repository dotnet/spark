// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Sql.Types
{
    /// <summary>
    /// Represents Timestamp containing year, month, day, hour, minute, second, microsecond in
    /// Coordinated Universal Time (UTC).
    /// </summary>
    public class Timestamp
    {
        private readonly DateTime _dateTime;
        private static readonly DateTime s_unixTimeEpoch =
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Constructor for Timestamp class.
        /// </summary>
        /// <param name="dateTime">DateTime object</param>
        public Timestamp(DateTime dateTime)
        {
            if (dateTime.Kind != DateTimeKind.Utc)
            {
                throw new InvalidTimeZoneException(
                    $"Invalid TimeZone for Timestamp, please use " +
                    $"Coordinated Universal Time (UTC) instead of {dateTime.Kind}.");
            }

            _dateTime = dateTime.ToUniversalTime();
        }

        /// <summary>
        /// Constructor for Timestamp class, the defaut timezone is
        /// Coordinated Universal Time (UTC).
        /// </summary>
        /// <param name="year">The year (1 through 9999)</param>
        /// <param name="month">The month (1 through 12)</param>
        /// <param name="day">The day (1 through the number of days in month)</param>
        /// <param name="hour">The hour (0 through 23)</param>
        /// <param name="minute">The minute (0 through 59)</param>
        /// <param name="second">The second (0 through 59)</param>
        /// <param name="microsecond">The microsecond (0 through 999999)</param>
        public Timestamp(
            int year,
            int month,
            int day,
            int hour,
            int minute,
            int second,
            int microsecond)
        {
            if (microsecond <= 999999 && microsecond >= 0)
            {
                // Create DateTime and AddTicks based on the microsecond value.
                _dateTime = new DateTime(year, month, day, hour, minute, second)
                    .AddTicks(
                        microsecond * (long)Math.Pow(10, 6 - microsecond.ToString().Length) * 10)
                    .ToUniversalTime();
            }
            else
            {
                throw new ArgumentOutOfRangeException($"Invalid microsecond value {microsecond}." +
                    $" The microsecond is from 0 through 999999.");
            }
        }

        /// <summary>
        /// Returns the year component of the timestamp.
        /// </summary>
        public int Year => _dateTime.Year;

        /// <summary>
        /// Returns the month component of the timestamp.
        /// </summary>
        public int Month => _dateTime.Month;

        /// <summary>
        /// Returns the day component of the timestamp.
        /// </summary>
        public int Day => _dateTime.Day;

        /// <summary>
        /// Returns the hour component of the timestamp.
        /// </summary>
        public int Hour => _dateTime.Hour;

        /// <summary>
        /// Returns the minute component of the timestamp.
        /// </summary>
        public int Minute => _dateTime.Minute;

        /// <summary>
        /// Returns the second component of the timestamp.
        /// </summary>
        public int Second => _dateTime.Second;

        /// <summary>
        /// Returns the microsecond component of the timestamp.
        /// </summary>
        public int Microsecond => (int)(_dateTime.Ticks -
            new DateTime(Year, Month, Day, Hour, Minute, Second).Ticks) / 10;

        /// <summary>
        /// Readable string representation for this type.
        /// </summary>
        public override string ToString() => _dateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

        /// <summary>
        /// Checks if the given object is same as the current object.
        /// </summary>
        /// <param name="obj">Other object to compare against</param>
        /// <returns>True if the other object is equal</returns>
        public override bool Equals(object obj) =>
            ReferenceEquals(this, obj) ||
            ((obj is Timestamp timestamp) && _dateTime.Equals(timestamp._dateTime));

        /// <summary>
        /// Returns the hash code of the current object.
        /// </summary>
        /// <returns>The hash code of the current object</returns>
        public override int GetHashCode() => _dateTime.GetHashCode();

        /// <summary>
        /// Returns DateTime object describing this type.
        /// </summary>
        /// <returns>DateTime object of the current object</returns>
        public DateTime ToDateTime() => _dateTime;

        /// <summary>
        /// Returns a double object that represents the number of microseconds from the epoch
        /// of 1970-01-01T00:00:00.000000Z(UTC+00:00) in the second unit to serialize and
        /// deserialize between CLR and JVM.
        /// </summary>
        /// <returns>Double object that represents the number of seconds from the epoch of
        /// 1970-01-01T00:00:00.000000Z(UTC+00:00)</returns>
        internal double GetIntervalInSeconds() => (_dateTime.Ticks - s_unixTimeEpoch.Ticks) /
            10000000.0;

        /// <summary>
        /// Returns a long object that represents the number of microseconds from the epoch of
        /// 1970-01-01T00:00:00.000000Z(UTC+00:00).
        /// </summary>
        /// <returns>Long object that represents the number of microseconds from the epoch of
        /// 1970-01-01T00:00:00.000000Z(UTC+00:00)</returns>
        internal long GetIntervalInMicroseconds() => (_dateTime.Ticks - s_unixTimeEpoch.Ticks) /
            10;
    }
}
