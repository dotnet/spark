using System;
using System.Linq;
using Microsoft.Spark.Sql;
using Xunit;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Serializable]
    public class TestBroadcastVariable
    {
        public int IntValue { get; private set; }
        public string StringValue { get; private set; }

        public TestBroadcastVariable(int intVal, string stringVal)
        {
            IntValue = intVal;
            StringValue = stringVal;
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
            _df = _spark.CreateDataFrame(new[] { "hello", "world" });
        }

        /// <summary>
        /// Test Broadcast support by using multiple broadcast variables in a UDF.
        /// </summary>
        [Theory]
        [InlineData("true")]
        [InlineData("false")]
        public void TestMultipleBroadcast(string isEncryptionEnabled)
        {
            _spark.SparkContext.GetConf().Set("spark.io.encryption.enabled", isEncryptionEnabled);
            var obj1 = new TestBroadcastVariable(1, "first");
            var obj2 = new TestBroadcastVariable(2, "second");
            Broadcast<TestBroadcastVariable> bc1 = _spark.SparkContext.Broadcast(obj1);
            Broadcast<TestBroadcastVariable> bc2 = _spark.SparkContext.Broadcast(obj2);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc1.Value().StringValue} and {bc2.Value().StringValue}");

            var expected = new string[] { "hello first and second", "world first and second" };

            string[] actual = ToStringArray(_df.Select(udf(_df["_1"])));
            Assert.Equal(expected, actual);
            bc1.Destroy();
            bc2.Destroy();
        }

        /// <summary>
        /// Test Broadcast.Unpersist() deletes cached copies of the broadcast on the executors. If
        /// the broadcast is used after unpersist is called, it is re-sent to the executors.
        /// </summary>
        [Theory]
        [InlineData("true")]
        [InlineData("false")]
        public void TestUnpersist(string isEncryptionEnabled)
        {
            _spark.SparkContext.GetConf().Set("spark.io.encryption.enabled", isEncryptionEnabled);
            var obj = new TestBroadcastVariable(1, "unpersist");
            Broadcast<TestBroadcastVariable> bc = _spark.SparkContext.Broadcast(obj);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc.Value().StringValue}, {bc.Value().IntValue}");

            var expected = new string[] { "hello unpersist, 1", "world unpersist, 1" };

            string[] actual = ToStringArray(_df.Select(udf(_df["_1"])));

            Assert.Equal(expected, actual);

            // This deletes the copies of the broadcast on the executors. We then use the Broadcast
            // variable again in the UDF and validate that it is re-sent to all executors.
            bc.Unpersist();

            string[] actualUnpersisted = ToStringArray(_df.Select(udf(_df["_1"])));
            Assert.Equal(expected, actualUnpersisted);
        }

        private string[] ToStringArray(DataFrame df)
        {
            Row[] rows = df.Collect().ToArray();
            return rows.Select(s => s[0].ToString()).ToArray();
        }
    }
}
