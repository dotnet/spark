using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.E2ETest.Utils;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Xunit;

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
        [Fact]
        public void TestMultipleBroadcastWithoutEncryption()
        {
            var obj1 = new TestBroadcastVariable(1, "first");
            var obj2 = new TestBroadcastVariable(2, "second");
            Broadcast<TestBroadcastVariable> bc1 = _spark.SparkContext.Broadcast(obj1);
            Broadcast<TestBroadcastVariable> bc2 = _spark.SparkContext.Broadcast(obj2);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc1.Value().StringValue} and {bc2.Value().StringValue}");

            var expected = new string[] {"hello first and second", "world first and second" };

            DataFrame udfResult = _df.Select(udf(_df["_1"]));
            string[] actual = DataFrameToString(udfResult);
            Assert.Equal(expected, actual);
        }

        /// <summary>
        /// Test Broadcast.Destroy() that destroys all data and metadata related to the broadcast
        /// variable and makes it inaccessible from workers.
        /// </summary>
        [Fact]
        public void TestDestroy()
        {
            var obj1 = new TestBroadcastVariable(5, "destroy");
            Broadcast<TestBroadcastVariable> bc1 = _spark.SparkContext.Broadcast(obj1);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc1.Value().StringValue}, {bc1.Value().IntValue}");

            var expected = new string[] {"hello destroy, 5", "world destroy, 5"};

            DataFrame udfResult = _df.Select(udf(_df["_1"]));
            string[] actual = DataFrameToString(udfResult);
            Assert.Equal(expected, actual);

            bc1.Destroy();

            try
            {
                Row[] testRows = _df.Select(udf(_df["_1"])).Collect().ToArray();
            }
            catch (Exception e)
            {
                Assert.NotNull(e);
            }
        }

        /// <summary>
        /// Test Broadcast.Unpersist() deletes cached copies of the broadcast on the executors. If
        /// the broadcast is used after unpersist is called, it is re-sent to the executors.
        /// </summary>
        [Fact]
        public void TestUnpersist()
        {
            var obj = new TestBroadcastVariable(1, "unpersist");
            Broadcast<TestBroadcastVariable> bc = _spark.SparkContext.Broadcast(obj);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc.Value().StringValue}, {bc.Value().IntValue}");

            var expected = new string[] {"hello unpersist, 1", "world unpersist, 1"};

            DataFrame udfResult = _df.Select(udf(_df["_1"]));
            string[] actual = DataFrameToString(udfResult);

            Assert.Equal(expected, actual);

            // This deletes the copies of the broadcast on the executors. We then use the Broadcast
            // variable again in the UDF and validate that it is re-sent to all executors.
            bc.Unpersist();

            DataFrame udfResultUnpersist = _df.Select(udf(_df["_1"]));
            string[] actualUnpersisted = DataFrameToString(udfResultUnpersist);
            Assert.Equal(expected, actualUnpersisted);
        }

        /// <summary>
        /// Function that converts the given DataFrame object to a list of strings.
        /// </summary>
        /// <param name="df"></param>
        /// <returns></returns>
        private string[] DataFrameToString(DataFrame df)
        {
            Row[] rows = df.Collect().ToArray();
            return rows.Select(s => s[0].ToString()).ToArray();
        }
    }
}
