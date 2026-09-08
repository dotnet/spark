using System;
using System.Linq;
using System.Runtime.InteropServices;
using MessagePack;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Xunit;
using Xunit.Abstractions;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.E2ETest.IpcTests
{
    [Serializable]
    public class TestBroadcastVariable
    {
        public int IntValue { get; private set; }
        public string StringValue { get; private set; }

        [SerializationConstructor]
        public TestBroadcastVariable(int intValue, string stringValue)
        {
            IntValue = intValue;
            StringValue = stringValue;
        }
    }

    [Collection("Spark E2E Tests")]
    [Trait("Category", "Broadcast")]
    public class BroadcastTests
    {
        private readonly SparkSession _spark;
        private readonly DataFrame _df;
        private readonly ITestOutputHelper _output;

        public BroadcastTests(SparkFixture fixture, ITestOutputHelper output)
        {
            _spark = fixture.Spark;
            _df = _spark.CreateDataFrame(new[] { "hello", "world" });
            _output = output;
        }

        /// <summary>
        /// Test Broadcast support by using multiple broadcast variables in a UDF.
        /// </summary>
        [Fact]
        public void TestMultipleBroadcast()
        {
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
        /// Test Broadcast.Destroy() that destroys all data and metadata related to the broadcast
        /// variable and makes it inaccessible from workers.
        /// </summary>
        [Fact]
        public void TestDestroy()
        {
            var obj1 = new TestBroadcastVariable(5, "destroy");
            Broadcast<TestBroadcastVariable> bc1 = _spark.SparkContext.Broadcast(obj1);

            Func<Column, Column> udf = CreateBroadcastUdf(bc1);

            var expected = new string[] { "hello destroy, 5", "world destroy, 5" };

            string[] actual = ToStringArray(_df.Select(udf(_df["_1"])));
            Assert.Equal(expected, actual);

            bc1.Destroy();

            Exception exception = Assert.ThrowsAny<Exception>(() =>
                _df.Select(udf(_df["_1"])).Collect().ToArray());
            Assert.Contains("destroyed", exception.ToString());
        }

        /// <summary>
        /// Test Broadcast.Unpersist() deletes cached copies of the broadcast on the executors. If
        /// the broadcast is used after unpersist is called, it is re-sent to the executors.
        /// </summary>
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestUnpersist(bool blocking)
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
            if (blocking)
            {
                bc.Unpersist(true);
            }
            else
            {
                bc.Unpersist();
            }

            string[] actualUnpersisted = ToStringArray(_df.Select(udf(_df["_1"])));
            Assert.Equal(expected, actualUnpersisted);
            bc.Destroy();
        }

        [Fact]
        public void EncryptionConfigurationMatchesRunningSparkEnvironment()
        {
            // Encryption must be set at spark-submit startup, not on GetConf()'s copy.
            bool configured = bool.Parse(
                _spark.SparkContext.GetConf().Get("spark.io.encryption.enabled", "false"));
            var sparkEnv = (JvmObjectReference)_spark.Reference.Jvm.CallStaticJavaMethod(
                "org.apache.spark.SparkEnv", "get");
            var serializer = (JvmObjectReference)sparkEnv.Invoke("serializerManager");
            bool actual = (bool)serializer.Invoke("encryptionEnabled");
            Assert.Equal(configured, actual);
            string expected = Environment.GetEnvironmentVariable(
                "DOTNET_SPARKFIXTURE_EXPECTED_IO_ENCRYPTION");
            if (expected != null)
            {
                Assert.Equal(bool.Parse(expected), actual);
            }
            _output.WriteLine($"Spark IO encryption: {actual}");
        }

        [Fact]
        public void RddBroadcastSetsChangeAcrossSuccessiveTasks()
        {
            Broadcast<int> first = _spark.SparkContext.Broadcast(10);
            Broadcast<int> second = _spark.SparkContext.Broadcast(100);
            try
            {
                RDD<int> input = _spark.SparkContext.Parallelize(new[] { 1, 2, 3, 4 }, 2);
                string[] firstRun = RunBroadcastJob(input, first);
                string[] secondRun = RunBroadcastJob(input, second);
                // This task removes all broadcasts from a reused worker.
                Assert.Equal(new[] { 1, 2, 3, 4 }, input.Map(value => value).Collect());
                string[] readded = RunBroadcastJob(input, first);
                Assert.Equal(new[] { 11, 12, 13, 14 }, BroadcastValues(firstRun));
                Assert.Equal(new[] { 101, 102, 103, 104 }, BroadcastValues(secondRun));
                Assert.Equal(new[] { 11, 12, 13, 14 }, BroadcastValues(readded));
                int[] pids = firstRun.Concat(secondRun).Concat(readded)
                    .Select(value => int.Parse(value.Split(':')[0])).ToArray();
                Assert.DoesNotContain(Environment.ProcessId, pids);
                _output.WriteLine($"Broadcast task PIDs: {string.Join(", ", pids)}");
                SparkConf conf = _spark.SparkContext.GetConf();
                if (SparkSettings.Version.Major == 4 &&
                    !RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
                    bool.Parse(conf.Get("spark.python.use.daemon", "true")) &&
                    bool.Parse(conf.Get("spark.python.worker.reuse", "true")))
                {
                    // The fixture uses one local task slot: prove actual process reuse.
                    Assert.Single(pids.Distinct());
                }
            }
            finally
            {
                first.Destroy();
                second.Destroy();
            }
        }

        [Fact]
        public void BroadcastWorkerFailureAllowsSubsequentTask()
        {
            Broadcast<int> broadcast = _spark.SparkContext.Broadcast(123);
            try
            {
                RDD<int> input = _spark.SparkContext.Parallelize(new[] { 1, 2 }, 2);
                RDD<int> failing = CreateFailingBroadcastRdd(input, broadcast);
                Exception exception = Assert.ThrowsAny<Exception>(() => failing.Collect().ToArray());
                Assert.Contains("broadcast-failure-123", exception.ToString());
                Assert.Equal(new[] { 124, 125 }, BroadcastValues(RunBroadcastJob(input, broadcast)));
            }
            finally
            {
                broadcast.Destroy();
            }
        }

        // Build worker delegates separately from assertion lambdas so their compiler-generated
        // closures contain only broadcasts, not the test instance or driver-side DataFrames/RDDs.
        private static Func<Column, Column> CreateBroadcastUdf(
            Broadcast<TestBroadcastVariable> broadcast) =>
            Udf<string, string>(str =>
                $"{str} {broadcast.Value().StringValue}, {broadcast.Value().IntValue}");

        private static RDD<int> CreateFailingBroadcastRdd(RDD<int> input, Broadcast<int> broadcast) =>
            input.Map<int>(value =>
                throw new InvalidOperationException($"broadcast-failure-{broadcast.Value()}"));

        private static string[] RunBroadcastJob(RDD<int> input, Broadcast<int> broadcast) =>
            input.Map(value => $"{Environment.ProcessId}:{value + broadcast.Value()}")
                .Collect().ToArray();

        private static int[] BroadcastValues(string[] rows) =>
            rows.Select(value => int.Parse(value.Split(':')[1])).ToArray();

        private string[] ToStringArray(DataFrame df)
        {
            Row[] rows = df.Collect().ToArray();
            return rows.Select(s => s[0].ToString()).ToArray();
        }
    }
}
