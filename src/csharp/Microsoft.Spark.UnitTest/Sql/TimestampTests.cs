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
                var timestamp = new Timestamp(
                    new DateTime(2020, 1, 2, 15, 30, 30, DateTimeKind.Utc));

                // Validate values.
                Assert.Equal(2020, timestamp.Year);
                Assert.Equal(1, timestamp.Month);
                Assert.Equal(2, timestamp.Day);
                Assert.Equal(15, timestamp.Hour);
                Assert.Equal(30, timestamp.Minute);
                Assert.Equal(30, timestamp.Second);

                // Validate ToString().
                Assert.Equal("2020-01-02 15:30:30.000000", timestamp.ToString());

                // Validate ToDateTime().
                Assert.IsType<DateTime>(timestamp.ToDateTime());

                // Validate TimeZone when using DateTime to create Timestamp objects.
                Assert.Throws<InvalidTimeZoneException>(
                    () => new Timestamp(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Local)));
            }

            {
                // Validate Microsecond and TimeZone.
                var timestamp = new Timestamp(2020, 1, 2, 15, 30, 30, 123456);

                Assert.Equal(123456, timestamp.Microsecond);
                Assert.Equal(DateTimeKind.Utc, timestamp.ToDateTime().Kind);
            }
        }
    }
}
