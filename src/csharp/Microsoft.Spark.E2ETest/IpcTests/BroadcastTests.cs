﻿using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Xunit;
using Xunit.Sdk;

namespace Microsoft.Spark.E2ETest.IpcTests
{

    [Serializable]
    public class TestBroadcastVariable
    {
        public int IntValue { get; set; }
        public string StringValue { get; set; }

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
            var data = new List<string>(new string[] { "hello ", "world " });
            _df = _spark.CreateDataFrame(data);
        }

        /// <summary>
        /// Test Broadcast support by using a single broadcast variable in a UDF.
        /// </summary>
        [Fact]
        public void TestSingleBroadcastWithoutEncryption()
        {
            var objectToBroadcast = new TestBroadcastVariable(
                1,
                "first broadcast");

            Broadcast<TestBroadcastVariable> bc = _spark.SparkContext.Broadcast(objectToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                str => str +
                (bc.Value()).StringValue +
                ", " +(bc.Value()).IntValue);

            string[] expected = new[] {
                "hello first broadcast, 1",
                "world first broadcast, 1" };

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
            var object1ToBroadcast = new TestBroadcastVariable(
                1,
                "first broadcast");
            var object2ToBroadcast = new TestBroadcastVariable(
                2,
                "second broadcast");

            Broadcast<TestBroadcastVariable> bc1 = _spark.SparkContext.Broadcast(object1ToBroadcast);
            Broadcast<TestBroadcastVariable> bc2 = _spark.SparkContext.Broadcast(object2ToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                str => str +
                (bc1.Value()).StringValue +
                " and " +(bc2.Value()).StringValue);

            string[] expected = new[] {
                "hello first broadcast and second broadcast",
                "world first broadcast and second broadcast" };

            Row[] actualRows = _df.Select(testBroadcast(_df["_1"])).Collect().ToArray();
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
            var objectToBroadcast = new TestBroadcastVariable(
                3,
                "Broadcast.Destroy()");

            Broadcast<TestBroadcastVariable> bc = _spark.SparkContext.Broadcast(objectToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                str => str +
                (bc.Value()).StringValue +
                ", " + (bc.Value()).IntValue);

            string[] expected = new[] {
                "hello Broadcast.Destroy(), 3",
                "world Broadcast.Destroy(), 3" };

            Row[] actualRows = _df.Select(testBroadcast(_df["_1"])).Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);

            bc.Destroy();

            try
            {
                Row[] rowsAfterDestroy = _df.Select(testBroadcast(_df["_1"])).Collect().ToArray();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Broadcast variable accessed after being destroyed: {e.Message}");
            }
        }

        [Fact]
        public void TestUnpersist()
        {
            var objectToBroadcast = new TestBroadcastVariable(
                4,
                "Broadcast.Unpersist()");

            Broadcast<TestBroadcastVariable> bc = _spark.SparkContext.Broadcast(objectToBroadcast);

            Func<Column, Column> testBroadcast = Udf<string, string>(
                str => str +
                (bc.Value()).StringValue +
                ", " + (bc.Value()).IntValue);

            string[] expected = new[] {
                "hello Broadcast.Unpersist(), 4",
                "world Broadcast.Unpersist(), 4" };

            Row[] actualRows = _df.Select(testBroadcast(_df["_1"])).Collect().ToArray();
            string[] actual = actualRows.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actual);

            bc.Unpersist();

            Func<Column, Column> testUnpersistedBroadcast = Udf<string, string>(
                str => str +
                (bc.Value()).StringValue +
                ", " + (bc.Value()).IntValue);

            Row[] rowsAfterUnpersist = _df.Select(testUnpersistedBroadcast(_df["_1"])).Collect().ToArray();
            string[] actualUnpersisted = rowsAfterUnpersist.Select(s => s[0].ToString()).ToArray();
            Assert.Equal(expected, actualUnpersisted);
        }
    }
}
