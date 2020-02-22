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
        public int _intValue { get; set; }
        public string _stringValue { get; set; }
        public double _doubleValue { get; set; }
        public bool _boolValue { get; set; }

        public BroadcastTestClass(int _int, string _string, double _double, bool _bool)
        {
            _intValue = _int;
            _stringValue = _string;
            _doubleValue = _double;
            _boolValue = _bool;
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
            var data = new List<string>(new string[] { "Alice", "Bob" });
            _df = _spark.CreateDataFrame(data);
        }

        /// <summary>
        /// Function tests Broadcast support by using a single broadcast variable in a UDF.
        /// </summary>
        [Fact]
        public void TestSingleBroadcastWithoutEncryption()
        {
            _spark.SparkContext.GetConf().Set("spark.io.encryption.enabled", "false");
            var objectToBroadcast = new BroadcastTestClass(
                1,
                "testing first broadcast",
                1.1,
                true);

            Broadcast bc = _spark.SparkContext.Broadcast(objectToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                row => row + " is " + 
                ((BroadcastTestClass)bc.Value())._stringValue + 
                ", " + ((BroadcastTestClass)bc.Value())._intValue + 
                ", " + ((BroadcastTestClass)bc.Value())._doubleValue + 
                " => " + ((BroadcastTestClass)bc.Value())._boolValue);

            var expected = new[] {
                "Alice is testing first broadcast, 1, 1.1 => True",
                "Bob is testing first broadcast, 1, 1.1 => True" };

            DataFrame udfOutput = _df.Select(testBroadcast(_df["_1"]));
            Row[] actualRows = udfOutput.Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);
            bc.Unpersist();
        }

        /// <summary>
        /// Function tests Broadcast support by using 2 broadcast variables in a UDF
        /// </summary>
        [Fact]
        public void TestMultipleBroadcastWithoutEncryption()
        {
            _spark.SparkContext.GetConf().Set("spark.io.encryption.enabled", "false");

            var object1ToBroadcast = new BroadcastTestClass(
                1,
                "testing first broadcast",
                1.1,
                true);
            var object2ToBroadcast = new BroadcastTestClass(
                2,
                "testing second broadcast",
                2.2,
                false);

            Broadcast bc1 = _spark.SparkContext.Broadcast(object1ToBroadcast);
            Broadcast bc2 = _spark.SparkContext.Broadcast(object2ToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                row => row + " is " +
                ((BroadcastTestClass)bc1.Value())._stringValue +
                " and " + ((BroadcastTestClass)bc2.Value())._stringValue);

            var expected = new[] {
                "Alice is testing first broadcast and testing second broadcast",
                "Bob is testing first broadcast and testing second broadcast" };

            DataFrame udfOutput = _df.Select(testBroadcast(_df["_1"]));
            Row[] actualRows = udfOutput.Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);
        }
    }
}
