// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Sql.Types
{
    /// <summary>
    /// Represents Date containing year, month, and day.
    /// </summary>
    public class Date
    {
        private readonly DateTime _dateTime;
        private static readonly DateTime s_unixTimeEpoch = new DateTime(1970, 1, 1);

        /// <summary>
        /// Constructor for Date class.
        /// </summary>
        /// <param name="dateTime">DateTime object</param>
        public Date(DateTime dateTime)
        {
            _dateTime = dateTime;
        }

        /// <summary>
        /// Constructor for Date class.
        /// </summary>
        /// <param name="year">The year (1 through 9999)</param>
        /// <param name="month">The month (1 through 12)</param>
        /// <param name="day">The day (1 through the number of days in month)</param>
        public Date(int year, int month, int day)
        {
            _dateTime = new DateTime(year, month, day);
        }

        /// <summary>
        /// Returns the year component of the date.
        /// </summary>
        public int Year => _dateTime.Year;

        /// <summary>
        /// Returns the month component of the date.
        /// </summary>
        public int Month => _dateTime.Month;

        /// <summary>
        /// Returns the day component of the date.
        /// </summary>
        public int Day => _dateTime.Day;

        /// <summary>
        /// Readable string representation for this type.
        /// </summary>
        public override string ToString() => _dateTime.ToString("yyyy-MM-dd");

        /// <summary>
        /// Checks if the given object is same as the current object.
        /// </summary>
        /// <param name="obj">Other object to compare against</param>
        /// <returns>True if the other object is equal.</returns>
        public override bool Equals(object obj) =>
            ReferenceEquals(this, obj) ||
            ((obj is Date date) && Year.Equals(date.Year) && Month.Equals(date.Month) &&
                Day.Equals(date.Day));

        /// <summary>
        /// Returns the hash code of the current object.
        /// </summary>
        /// <returns>The hash code of the current object</returns>
        public override int GetHashCode() => base.GetHashCode();

        /// <summary>
        /// Returns DateTime object describing this type.
        /// </summary>
        /// <returns>DateTime object of the current object</returns>
        public DateTime ToDateTime() => _dateTime;

        /// <summary>
        /// Returns an integer object that represents a count of days from 1970-01-01.
        /// </summary>
        /// <returns>Integer object that represents a count of days from 1970-01-01.</returns>
        internal int GetInterval() => (_dateTime - s_unixTimeEpoch).Days;
    }
}
