// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.ML.Linalg;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests.ML.Linalg
{
    [Collection("Spark E2E Tests")]
    public class SparseVectorTests
    {
        private readonly SparkSession _spark;

        public SparseVectorTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Test that we can create SparseVector, collect it back to .NET and use it in a UDF.
        /// </summary>
        [Fact]
        public void TestSparseVectorE2EScenario()
        {
            const int vectorLength = 10000;
            int[] indices = new[] { 1, 2, 3, 1000, 1001, 1002 };
            double[] values = new double[] { 100.0, 200.0, 300.0, 1100.0, 1101.1, 1102.2 };

            var vector = new SparseVector(vectorLength, indices, values);

            var schema = new StructType(new List<StructField>()
            {
                new StructField("name", new StringType()),
                new StructField("hectorTheVector", new SparseVectorUDT())
            });

            DataFrame dataFrame =
                _spark.CreateDataFrame(new List<GenericRow>() 
                { new GenericRow(new object[] { "hector", vector }) }, schema);

            dataFrame.Show();

            Row row = dataFrame.Collect().First();
            Row rowVector = row.GetAs<Row>(1);

            Assert.IsType<SparseVector>(SparseVector.FromRow(rowVector));

            Func<Column, Column> udf = Functions.Udf<Row, double>(udfRow =>
            {
                SparseVector udfVector = SparseVector.FromRow(udfRow);
                Assert.IsType<double>(udfVector.Apply(100));
                return udfVector.Apply(1000);
            });

            dataFrame.Select(udf(Functions.Col("hectorTheVector"))).Show();
        }

        /// <summary>
        /// Tests the methods available on SparseVector.
        /// </summary>
        [Fact]
        public void TestSparseVector()
        {
            const int vectorLength = 10000;
            int[] indices = new[] { 1, 2, 3, 1000, 1001, 1002 };
            double[] values = new double[] { 100.0, 200.0, 300.0, 1100.0, 1101.1, 1102.2 };

            var vector = new SparseVector(vectorLength, indices, values);

            Assert.Equal(values[2], vector.Apply(3));
            Assert.Equal(0.0, vector.Apply(9999));
            Assert.IsType<int>(vector.ArgMax());
            Assert.IsType<int>(vector.NumActives());
            Assert.IsType<int>(vector.NumNonZeros());
            Assert.Equal(vectorLength, vector.ToArray().Length);
            Assert.IsType<string>(vector.ToString());
        }
        
        /// <summary>
        /// Test that we can create a SparseVectorUDT.
        /// </summary>
        [Fact]
        public void TestSparseVectorUDT()
        {
            var type = new SparseVectorUDT();
            Assert.IsType<JObject>(type.JsonValue);
            Assert.IsType<string>(type.Json);
            Assert.IsType<string>(type.SimpleString);
            Assert.IsType<string>(type.TypeName);
            Assert.True(type.NeedConversion());
        }
    }
}
