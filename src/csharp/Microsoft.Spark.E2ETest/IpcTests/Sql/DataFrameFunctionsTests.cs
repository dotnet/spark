// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Xunit;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Collection("Spark E2E Tests")]
    public class DataFrameFunctionsTests
    {
        private readonly SparkSession _spark;
        private readonly DataFrame _df;

        public DataFrameFunctionsTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            _df = _spark
                .Read()
                .Json($"{TestEnvironment.ResourceDirectory}people.json");
        }

        [Fact]
        public void TestDataFrameNaFunctionSignatures()
        {
            DataFrameNaFunctions dfNaFuncs = _df.Na();

            var emptyColumn = new string[] { };
            var validColumn = new string[] { "age" };

            DataFrame df = dfNaFuncs.Drop("any");
            df = dfNaFuncs.Drop("all");
            df = dfNaFuncs.Drop(emptyColumn);
            df = dfNaFuncs.Drop(validColumn);
            df = dfNaFuncs.Drop("any", emptyColumn);
            df = dfNaFuncs.Drop("all", validColumn);
            df = dfNaFuncs.Drop(20);
            df = dfNaFuncs.Drop(20, emptyColumn);
            df = dfNaFuncs.Drop(20, validColumn);

            df = dfNaFuncs.Fill(100L);
            df = dfNaFuncs.Fill(100.0);
            df = dfNaFuncs.Fill("hello");
            df = dfNaFuncs.Fill(false);
            df = dfNaFuncs.Fill(100L, emptyColumn);
            df = dfNaFuncs.Fill(100L, validColumn);
            df = dfNaFuncs.Fill(100.0, emptyColumn);
            df = dfNaFuncs.Fill(100.0, validColumn);
            df = dfNaFuncs.Fill("hello", emptyColumn);
            df = dfNaFuncs.Fill("hello", validColumn);
            df = dfNaFuncs.Fill(true, emptyColumn);
            df = dfNaFuncs.Fill(true, validColumn);
            df = dfNaFuncs.Fill(new Dictionary<string, int>() { { "age", 10 } });
            df = dfNaFuncs.Fill(new Dictionary<string, long>() { { "age", 10L } });
            df = dfNaFuncs.Fill(new Dictionary<string, double>() { { "age", 10.0 } });
            df = dfNaFuncs.Fill(new Dictionary<string, string>() { { "age", "name" } });
            df = dfNaFuncs.Fill(new Dictionary<string, bool>() { { "age", false } });

            var doubleReplacement = new Dictionary<double, double>() { { 1.0, 5.0 } };
            var boolReplacement = new Dictionary<bool, bool>() { { true, false } };
            var stringReplacement = new Dictionary<string, string>() { { "a", "b" } };

            df = dfNaFuncs.Replace("age", doubleReplacement);
            df = dfNaFuncs.Replace("age", boolReplacement);
            df = dfNaFuncs.Replace("age", stringReplacement);
            df = dfNaFuncs.Replace(emptyColumn, doubleReplacement);
            df = dfNaFuncs.Replace(validColumn, doubleReplacement);
            df = dfNaFuncs.Replace(emptyColumn, boolReplacement);
            df = dfNaFuncs.Replace(validColumn, boolReplacement);
            df = dfNaFuncs.Replace(emptyColumn, stringReplacement);
            df = dfNaFuncs.Replace(validColumn, stringReplacement);
        }

        [Fact]
        public void TestDataFrameStatFunctionSignatures()
        {
            DataFrameStatFunctions stat = _df.Stat();

            double[] result = stat.ApproxQuantile("age", new[] { 0.5, 0.5 }, 0.3);

            double cov = stat.Cov("age", "age");

            double corr = stat.Corr("age", "age", "pearson");
            corr = stat.Corr("age", "age");

            var columnNames = new string[] { "age", "name" };
            DataFrame df = stat.FreqItems(columnNames, 0.2);
            df = stat.FreqItems(columnNames);

            df = stat.SampleBy("age", new Dictionary<int, double> { { 1, 0.5 } }, 100);
        }

        /// <summary>
        /// Test signatures for APIs introduced in Spark 3.0.*.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V3_0_0)]
        public void TestSignaturesV3_0_X()
        {
            DataFrameStatFunctions stat = _df.Stat();
            Column col = Column("age");

            Assert.IsType<DataFrame>(stat.SampleBy(
                col,
                new Dictionary<int, double> { { 1, 0.5 } },
                100));
        }
    }
}
