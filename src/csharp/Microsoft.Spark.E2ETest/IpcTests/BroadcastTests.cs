using System;
using System.Linq;
using Microsoft.Spark.E2ETest.Utils;
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
        /// Test Broadcast support by using multiple broadcast variables in a UDF with
        /// encryption enabled.
        /// This test is filtered out when backward compatibility tests are run.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_3_2)]
        public void TestMultipleBroadcastWithEncryption()
        {
            _spark.SparkContext.GetConf().Set("spark.io.encryption.enabled", "true");
            var obj1 = new TestBroadcastVariable(1, "first");
            var obj2 = new TestBroadcastVariable(2, "second");
            Broadcast<int> bc = _spark.SparkContext.Broadcast(5);
            Broadcast<TestBroadcastVariable> bc1 = _spark.SparkContext.Broadcast(obj1);
            Broadcast<TestBroadcastVariable> bc2 = _spark.SparkContext.Broadcast(obj2);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc1.Value().StringValue} and {bc2.Value().StringValue}");

            var expected = new string[] { "hello first and second", "world first and second" };

            string[] actual = ToStringArray(_df.Select(udf(_df["_1"])));
            Assert.Equal(expected, actual);
        }

        /// <summary>
        /// Test Broadcast support by using multiple broadcast variables in a UDF.
        /// This test is filtered out when backward compatibility tests are run.
        /// </summary>
        [Fact]
        public void TestMultipleBroadcastWithoutEncryption()
        {
            _spark.SparkContext.GetConf().Set("spark.io.encryption.enabled", "false");
            var obj1 = new TestBroadcastVariable(1, "first");
            var obj2 = new TestBroadcastVariable(2, "second");
            Broadcast<TestBroadcastVariable> bc1 = _spark.SparkContext.Broadcast(obj1);
            Broadcast<TestBroadcastVariable> bc2 = _spark.SparkContext.Broadcast(obj2);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc1.Value().StringValue} and {bc2.Value().StringValue}");

            var expected = new string[] { "hello first and second", "world first and second" };

            string[] actual = ToStringArray(_df.Select(udf(_df["_1"])));
            Assert.Equal(expected, actual);
        }

        /// <summary>
        /// Test Broadcast with encryption support by broadcasting a large (>100MB) object.
        /// This test is filtered out when backward compatibility tests are run.
        /// </summary>
        [SkipIfSparkVersionIsLessThan(Versions.V2_3_2)]
        public void TestLargeBroadcastValueWithEncryption()
        {
            var obj1 = new byte[104858000];
            Broadcast<byte[]> bc1 = _spark.SparkContext.Broadcast(obj1);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str}: length of broadcast array = {bc1.Value().Length}");

            var expected = new string[] {
                "hello: length of broadcast array = 104858000",
                "world: length of broadcast array = 104858000" };

            string[] actual = ToStringArray(_df.Select(udf(_df["_1"])));
            Assert.Equal(expected, actual);
        }

        /// <summary>
        /// Test Broadcast.Destroy() that destroys all data and metadata related to the broadcast
        /// variable and makes it inaccessible from workers.
        /// This test is filtered out when backward compatibility tests are run.
        /// </summary>
        [Fact]
        public void TestDestroy()
        {
            var obj1 = new TestBroadcastVariable(5, "destroy");
            Broadcast<TestBroadcastVariable> bc1 = _spark.SparkContext.Broadcast(obj1);

            Func<Column, Column> udf = Udf<string, string>(
                str => $"{str} {bc1.Value().StringValue}, {bc1.Value().IntValue}");

            var expected = new string[] { "hello destroy, 5", "world destroy, 5" };

            string[] actual = ToStringArray(_df.Select(udf(_df["_1"])));
            Assert.Equal(expected, actual);

            bc1.Destroy();

            // Throws the following exception:
            // ERROR Utils: Exception encountered
            //  org.apache.spark.SparkException: Attempted to use Broadcast(0) after it was destroyed(destroy at NativeMethodAccessorImpl.java:0)
            //  at org.apache.spark.broadcast.Broadcast.assertValid(Broadcast.scala:144)
            //  at org.apache.spark.broadcast.TorrentBroadcast$$anonfun$writeObject$1.apply$mcV$sp(TorrentBroadcast.scala:203)
            //  at org.apache.spark.broadcast.TorrentBroadcast$$anonfun$writeObject$1.apply(TorrentBroadcast.scala:202)
            //  at org.apache.spark.broadcast.TorrentBroadcast$$anonfun$writeObject$1.apply(TorrentBroadcast.scala:202)
            //  at org.apache.spark.util.Utils$.tryOrIOException(Utils.scala:1326)
            //  at org.apache.spark.broadcast.TorrentBroadcast.writeObject(TorrentBroadcast.scala:202)
            //  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
            try
            {
                _df.Select(udf(_df["_1"])).Collect().ToArray();
                Assert.True(false);
            }
            catch (Exception e)
            {
                Assert.NotNull(e);
            }
        }

        /// <summary>
        /// Test Broadcast.Unpersist() deletes cached copies of the broadcast on the executors. If
        /// the broadcast is used after unpersist is called, it is re-sent to the executors.
        /// This test is filtered out when backward compatibility tests are run.
        /// </summary>
        [Fact]
        public void TestUnpersist()
        {
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
