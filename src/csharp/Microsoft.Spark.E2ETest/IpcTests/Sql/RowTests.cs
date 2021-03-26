// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests

{
    [Collection("Spark E2E Tests")]
    public class RowTests
    {
        private readonly SparkSession _spark;

        public RowTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        [Fact]
        public void TestWithDuplicatedRows()
        {
            var schema = new StructType(new StructField[]
            {
                new StructField("ts", new TimestampType())
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[] { new Timestamp(2020, 1, 1, 0, 0, 0, 0) })
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);

            df.WithColumn("tsRow", Struct("ts")).WithColumn("tsRowRow", Struct("tsRow")).Collect().ToArray();
        }

        [Fact]
        public void TestWithDuplicateTimestamps()
        {
            var schema = new StructType(new StructField[]
            {
                new StructField("ts", new TimestampType())
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[] { new Timestamp(2020, 1, 1, 0, 0, 0, 0) }),
                new GenericRow(new object[] { new Timestamp(2020, 1, 1, 0, 0, 0, 0) }),
                new GenericRow(new object[] { new Timestamp(2020, 1, 1, 0, 0, 0, 0) })
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);

            df.Collect().ToArray();
        }

        [Fact]
        public void TestWithDuplicateDates()
        {
            var schema = new StructType(new StructField[]
            {
                new StructField("date", new DateType())
            });

            var data = new GenericRow[]
            {
                new GenericRow(new object[] { new Date(2020, 1, 1) }),
                new GenericRow(new object[] { new Date(2020, 1, 1) }),
                new GenericRow(new object[] { new Date(2020, 1, 1) })
            };

            DataFrame df = _spark.CreateDataFrame(data, schema);

            df.Collect().ToArray();
        }
    }
}
