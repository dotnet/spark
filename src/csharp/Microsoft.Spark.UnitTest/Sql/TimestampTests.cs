// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class TimestampTests
    {
        [Fact]
        public void TimestampTest()
        {
            {
                var testDate =
                    new DateTime(2020, 1, 1, 8, 30, 30, DateTimeKind.Utc).AddTicks(1230);
                var timestamp = new Timestamp(testDate);
                
                // Validate values.
                Assert.Equal(2020, timestamp.Year);
                Assert.Equal(1, timestamp.Month);
                Assert.Equal(1, timestamp.Day);
                Assert.Equal(8, timestamp.Hour);
                Assert.Equal(30, timestamp.Minute);
                Assert.Equal(30, timestamp.Second);
                Assert.Equal(123, timestamp.Microsecond);
                Assert.Equal(DateTimeKind.Utc, timestamp.ToDateTime().Kind);

                // Validate ToString().
                Assert.Equal("2020-01-01 08:30:30.000123Z", timestamp.ToString());

                // Validate ToDateTime().
                Assert.Equal(testDate, timestamp.ToDateTime());
            }

            {
                // Validate TimeZone.
                var timestamp = new Timestamp(
                    new DateTime(2020, 1, 1, 8, 30, 30, DateTimeKind.Local));

                Assert.Equal(DateTimeKind.Utc, timestamp.ToDateTime().Kind);
            }

            {
                var timestamp = new Timestamp(2020, 1, 2, 15, 30, 30, 123456);

                // Validate values.
                Assert.Equal(2020, timestamp.Year);
                Assert.Equal(1, timestamp.Month);
                Assert.Equal(2, timestamp.Day);
                Assert.Equal(15, timestamp.Hour);
                Assert.Equal(30, timestamp.Minute);
                Assert.Equal(30, timestamp.Second);
                Assert.Equal(123456, timestamp.Microsecond);
                Assert.Equal(DateTimeKind.Utc, timestamp.ToDateTime().Kind);

                // Validate ToString().
                Assert.Equal("2020-01-02 15:30:30.123456Z", timestamp.ToString());

                // Validate ToDateTime().
                Assert.Equal(
                    new DateTime(2020, 1, 2, 15, 30, 30, DateTimeKind.Utc).AddTicks(1234560),
                    timestamp.ToDateTime());
            }

            {
                // Validate microsecond values.
                Assert.Throws<ArgumentOutOfRangeException>(
                    () => new Timestamp(2020, 1, 2, 15, 30, 30, 1234567));
            }
        }

        [Fact]
        public void TestTimestampToString()
        {
            var dateTimeObj = new DateTime(2021, 01, 01);
            Assert.Equal(
                new Timestamp(DateTime.Parse(new Timestamp(dateTimeObj).ToString())),
                new Timestamp(dateTimeObj));
        }
    }
}
