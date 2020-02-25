using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Xunit;

namespace Microsoft.Spark.E2ETest.UdfTests
{
    [Serializable]
    public class BroadcastTestClass
    {
        public int intValue { get; set; }
        public string stringValue { get; set; }
        public double doubleValue { get; set; }
        public bool boolValue { get; set; }

        public BroadcastTestClass(int _int, string _string, double _double, bool _bool)
        {
            intValue = _int;
            stringValue = _string;
            doubleValue = _double;
            boolValue = _bool;
        }
    }

    [Collection("Spark E2E Tests")]
    public class BroadcastTests
    {
        private readonly SparkSession _spark;
        private readonly DataFrame _df;

        public BroadcastTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            var data = new List<string>(new string[] { "Alice is testing: ", "Bob is testing: " });
            _df = _spark.CreateDataFrame(data);
        }

        /// <summary>
        /// Test Broadcast support by using a single broadcast variable in a UDF.
        /// </summary>
        [Fact]
        public void TestSingleBroadcastWithoutEncryption()
        {
            var objectToBroadcast = new BroadcastTestClass(
                1,
                "first broadcast",
                1.1,
                true);

            Broadcast bc = _spark.SparkContext.Broadcast(objectToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                str => str +
                ((BroadcastTestClass)bc.Value()).stringValue +
                ", " +((BroadcastTestClass)bc.Value()).intValue +
                ", " +((BroadcastTestClass)bc.Value()).doubleValue +
                ", " +((BroadcastTestClass)bc.Value()).boolValue);

            string[] expected = new[] {
                "Alice is testing: first broadcast, 1, 1.1, True",
                "Bob is testing: first broadcast, 1, 1.1, True" };

            Row[] actualRows = _df.Select(testBroadcast(_df["_1"])).Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);
        }

        /// <summary>
        /// Test Broadcast support by using multiple broadcast variables in a UDF.
        /// </summary>
        [Fact]
        public void TestMultipleBroadcastWithoutEncryption()
        {
            var object1ToBroadcast = new BroadcastTestClass(
                1,
                "first broadcast",
                1.1,
                true);
            var object2ToBroadcast = new BroadcastTestClass(
                2,
                "second broadcast",
                2.2,
                false);

            Broadcast bc1 = _spark.SparkContext.Broadcast(object1ToBroadcast);
            Broadcast bc2 = _spark.SparkContext.Broadcast(object2ToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                str => str +
                ((BroadcastTestClass)bc1.Value()).stringValue +
                " and " +((BroadcastTestClass)bc2.Value()).stringValue);

            string[] expected = new[] {
                "Alice is testing: first broadcast and second broadcast",
                "Bob is testing: first broadcast and second broadcast" };

            Row[] actualRows = _df.Select(testBroadcast(_df["_1"])).Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);

        }
    }
}
