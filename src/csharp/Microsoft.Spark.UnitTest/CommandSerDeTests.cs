// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class CommandSerDeTests
    {
        [Fact]
        public void TestCommandSerDeForSqlPickling()
        {
            var udfWrapper = new Sql.PicklingUdfWrapper<string, string>((str) => $"hello {str}");
            var workerFunction = new Sql.PicklingWorkerFunction(udfWrapper.Execute);

            var serializedCommand = Utils.CommandSerDe.Serialize(
                workerFunction.Func,
                Utils.CommandSerDe.SerializedMode.Row,
                Utils.CommandSerDe.SerializedMode.Row);

            using (var ms = new MemoryStream(serializedCommand))
            {
                var deserializedWorkerFunction = new Sql.PicklingWorkerFunction(
                    Utils.CommandSerDe.Deserialize<Sql.PicklingWorkerFunction.ExecuteDelegate>(
                        ms,
                        out Utils.CommandSerDe.SerializedMode serializerMode,
                        out Utils.CommandSerDe.SerializedMode deserializerMode,
                        out var runMode));

                Assert.Equal(Utils.CommandSerDe.SerializedMode.Row, serializerMode);
                Assert.Equal(Utils.CommandSerDe.SerializedMode.Row, deserializerMode);
                Assert.Equal("N", runMode);

                var result = deserializedWorkerFunction.Func(0, new[] { "spark" }, new[] { 0 });
                Assert.Equal("hello spark", result);
            }
        }

        [Fact]
        public void TestCommandSerDeForSqlArrow()
        {
            var udfWrapper = new ArrowUdfWrapper<string, string>((str) => $"hello {str}");
            var workerFunction = new ArrowWorkerFunction(udfWrapper.Execute);

            var serializedCommand = Utils.CommandSerDe.Serialize(
                workerFunction.Func,
                Utils.CommandSerDe.SerializedMode.Row,
                Utils.CommandSerDe.SerializedMode.Row);

            using (var ms = new MemoryStream(serializedCommand))
            {
                var deserializedWorkerFunction = new ArrowWorkerFunction(
                    Utils.CommandSerDe.Deserialize<ArrowWorkerFunction.ExecuteDelegate>(
                        ms,
                        out Utils.CommandSerDe.SerializedMode serializerMode,
                        out Utils.CommandSerDe.SerializedMode deserializerMode,
                        out var runMode));

                Assert.Equal(Utils.CommandSerDe.SerializedMode.Row, serializerMode);
                Assert.Equal(Utils.CommandSerDe.SerializedMode.Row, deserializerMode);
                Assert.Equal("N", runMode);

                Apache.Arrow.IArrowArray input = ArrowArrayHelpers.ToArrowArray(new[] { "spark" });
                Apache.Arrow.IArrowArray result =
                    deserializedWorkerFunction.Func(0, new[] { input }, new[] { 0 });
                ArrowTestUtils.AssertEquals("hello spark", result);
            }
        }

        [Fact]
        public void TestCommandSerDeForRDD()
        {
            // Construct the UDF tree such that func1, func2, and func3
            // are executed in that order.
            var func1 = new RDD.WorkerFunction(
                new RDD<int>.MapUdfWrapper<int, int>((a) => a + 3).Execute);

            var func2 = new RDD.WorkerFunction(
                new RDD<int>.MapUdfWrapper<int, int>((a) => a * 2).Execute);

            var func3 = new RDD.WorkerFunction(
                new RDD<int>.MapUdfWrapper<int, int>((a) => a + 5).Execute);

            var chainedFunc1 = RDD.WorkerFunction.Chain(func1, func2);
            var chainedFunc2 = RDD.WorkerFunction.Chain(chainedFunc1, func3);

            var serializedCommand = Utils.CommandSerDe.Serialize(
                chainedFunc2.Func,
                Utils.CommandSerDe.SerializedMode.Byte,
                Utils.CommandSerDe.SerializedMode.Byte);

            using (var ms = new MemoryStream(serializedCommand))
            {
                var deserializedWorkerFunction = new RDD.WorkerFunction(
                    Utils.CommandSerDe.Deserialize<RDD.WorkerFunction.ExecuteDelegate>(
                        ms,
                        out Utils.CommandSerDe.SerializedMode serializerMode,
                        out Utils.CommandSerDe.SerializedMode deserializerMode,
                        out var runMode));

                Assert.Equal(Utils.CommandSerDe.SerializedMode.Byte, serializerMode);
                Assert.Equal(Utils.CommandSerDe.SerializedMode.Byte, deserializerMode);
                Assert.Equal("N", runMode);

                IEnumerable<object> result =
                    deserializedWorkerFunction.Func(0, new object[] { 1, 2, 3 });
                Assert.Equal(new[] { 13, 15, 17 }, result.Cast<int>());
            }
        }
    }
}
