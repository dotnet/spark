using System;
using System.Collections.Generic;
using System.Linq;
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

            string[] expected = new[] {"hello first and second", "world first and second" };

            Row[] actualRows = _df.Select(udf(_df["_1"])).Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);

        }

        /// <summary>
        /// Test Broadcast.Destroy() that destroys all data and metadata related to the broadcast
        /// variable and makes it inaccessible from workers.
        /// </summary>
        [Fact]
        public void TestDestroy()
        {
            var obj1 = new TestBroadcastVariable(1, "first");
            var obj2 = new TestBroadcastVariable(2, "second");
            Broadcast<TestBroadcastVariable> bc1 = _spark.SparkContext.Broadcast(obj1);
            Broadcast<TestBroadcastVariable> bc2 = _spark.SparkContext.Broadcast(obj2);
            Exception expectedException = null;

            Func<Column, Column> udf1 = Udf<string, string>(
                str => $"{str} {bc1.Value().StringValue}, {bc1.Value().IntValue}");

            string[] expected = new[] { "hello first, 1", "world first, 1" };

            Row[] actualRows = _df.Select(udf1(_df["_1"])).Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);

            bc1.Destroy();

            try
            {
                Row[] testRows = _df.Select(udf1(_df["_1"])).Collect().ToArray();
            }
            catch (Exception e)
            {
                expectedException = e;
            }
            Assert.NotNull(expectedException);

            Func<Column, Column> udf2 = Udf<string, string>(
                str => $"{str} {bc2.Value().StringValue}, {bc2.Value().IntValue}");

            string[] expected2 = new[] { "hello second, 2", "world second, 2" };

            Row[] actualRows2 = _df.Select(udf2(_df["_1"])).Collect().ToArray();
            string[] actual2 = actualRows2.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected2, actual2);
        }

        /// <summary>
        /// Test Broadcast.Unpersist() deletes cached copies of the broadcast on the executors.
        /// </summary>
        [Fact]
        public void TestUnpersist()
        {
            var obj = new TestBroadcastVariable(
                4,
                "Broadcast.Unpersist()");

            Broadcast<TestBroadcastVariable> bc = _spark.SparkContext.Broadcast(obj);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc.Value().StringValue}, {bc.Value().IntValue}");

            string[] expected = new[] {
                "hello Broadcast.Unpersist(), 4",
                "world Broadcast.Unpersist(), 4" };

            Row[] actualRows = _df.Select(udf(_df["_1"])).Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);

            bc.Unpersist();

            Row[] rowsAfterUnpersist = _df.Select(udf(_df["_1"])).Collect().ToArray();
            string[] actualUnpersisted = rowsAfterUnpersist.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actualUnpersisted);
        }
    }
}
