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
        /// <summary>
        /// Constructor for Date class.
        /// </summary>
        /// <param name="dt">DateTime object</param>
        public Date(DateTime dt)
        {
            Year = dt.Year;
            Month = dt.Month;
            Day = dt.Day;
        }

        /// <summary>
        /// Constructor for Date class.
        /// </summary>
        /// <param name="year">The year (1 through 9999)</param>
        /// <param name="month">The month (1 through 12)</param>
        /// <param name="day">The day (1 through the number of days in month)</param>
        public Date(int year, int month, int day)
        {
            Year = year;
            Month = month;
            Day = day;
        }

        /// <summary>
        /// Returns the year component of the date.
        /// </summary>
        public int Year { get; private set; }

        /// <summary>
        /// Returns the month component of the date.
        /// </summary>
        public int Month { get; private set; }

        /// <summary>
        /// Returns the day component of the date.
        /// </summary>
        public int Day { get; private set; }

        /// <summary>
        /// Readable string representation for this type.
        /// </summary>
        public override string ToString() => $"{Month}/{Day}/{Year}";

        /// <summary>
        /// Readable string representation for this type using the specified format.
        /// </summary>
        /// <param name="format">A standard or custom date and time format string</param>
        public string ToString(string format) => new DateTime(Year, Month, Day).ToString(format);

        /// <summary>
        /// Returns DateTime object describing this type.
        /// </summary>
        public DateTime ToDateTime() => new DateTime(Year, Month, Day);

        /// <summary>
        /// Returns an integer object that represents a count of days from 1970-01-01.
        /// </summary>
        internal int GetInterval()
        {
            var unixTimeEpoch = new DateTime(1970, 1, 1);
            return (new DateTime(Year, Month, Day) - unixTimeEpoch).Days;
        }
    }
}
