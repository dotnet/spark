// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{

    [Collection("Spark E2E Tests")]    
    public class DataTypesTests
    {
        private readonly SparkSession _spark;

        public DataTypesTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Tests that we can pass a decimal over to Apache Spark and collect it back again, include a check
        /// for the minimum and maximum decimal that .NET can represent
        /// </summary>
        [Fact]
        public void TestDecimalType()
        {
            var df = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(
                        new object[]
                        {
                            decimal.MinValue, decimal.MaxValue, decimal.Zero, decimal.MinusOne,
                            new object[]
                            {
                                decimal.MinValue, decimal.MaxValue, decimal.Zero, decimal.MinusOne
                            }
                        }),
                },
                new StructType(
                    new List<StructField>()
                    {
                        new StructField("min", new DecimalType(38, 0)),
                        new StructField("max", new DecimalType(38, 0)),
                        new StructField("zero", new DecimalType(38, 0)),
                        new StructField("minusOne", new DecimalType(38, 0)),
                        new StructField("array", new ArrayType(new DecimalType(38,0)))
                    }));

            Row row = df.Collect().First();
            Assert.Equal(decimal.MinValue, row[0]);
            Assert.Equal(decimal.MaxValue, row[1]);
            Assert.Equal(decimal.Zero, row[2]);
            Assert.Equal(decimal.MinusOne, row[3]);
            Assert.Equal(new object[]{decimal.MinValue, decimal.MaxValue, decimal.Zero, decimal.MinusOne}, 
                row[4]);
        }

    }
}
