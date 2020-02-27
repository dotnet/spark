using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Xunit;

namespace Microsoft.Spark.E2ETest.UdfTests
{
    [Serializable]
    public class BroadcastExampleType
    {
        public int IntValue { get; set; }
        public string StringValue { get; set; }
        public double DoubleValue { get; set; }
        public bool BoolValue { get; set; }

        public BroadcastExampleType(int intVal, string stringVal, double doubleVal, bool boolVal)
        {
            IntValue = intVal;
            StringValue = stringVal;
            DoubleValue = doubleVal;
            BoolValue = boolVal;
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
            var objectToBroadcast = new BroadcastExampleType(
                1,
                "first broadcast",
                1.1,
                true);

            Broadcast bc = _spark.SparkContext.Broadcast(objectToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                str => str +
                ((BroadcastExampleType)bc.Value()).StringValue +
                ", " +((BroadcastExampleType)bc.Value()).IntValue +
                ", " +((BroadcastExampleType)bc.Value()).DoubleValue +
                ", " +((BroadcastExampleType)bc.Value()).BoolValue);

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
            var object1ToBroadcast = new BroadcastExampleType(
                1,
                "first broadcast",
                1.1,
                true);
            var object2ToBroadcast = new BroadcastExampleType(
                2,
                "second broadcast",
                2.2,
                false);

            Broadcast bc1 = _spark.SparkContext.Broadcast(object1ToBroadcast);
            Broadcast bc2 = _spark.SparkContext.Broadcast(object2ToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                str => str +
                ((BroadcastExampleType)bc1.Value()).StringValue +
                " and " +((BroadcastExampleType)bc2.Value()).StringValue);

            string[] expected = new[] {
                "Alice is testing: first broadcast and second broadcast",
                "Bob is testing: first broadcast and second broadcast" };

            Row[] actualRows = _df.Select(testBroadcast(_df["_1"])).Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);

        }
    }
}
