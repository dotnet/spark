// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;
using static Microsoft.Spark.UnitTest.TestUtils.ArrowTestUtils;

namespace Microsoft.Spark.UnitTest
{
    public class WorkerFunctionTests
    {
        [Fact]
        public void TestPicklingWorkerFunction()
        {
            var func = new PicklingWorkerFunction(
                new PicklingUdfWrapper<string, string>(
                    (str) => str).Execute);

            string[] input = { "arg1" };
            Assert.Equal(input[0], func.Func(0, input, new[] { 0 }));
        }

        [Fact]
        public void TestChainingPicklingWorkerFunction()
        {
            var func1 = new PicklingWorkerFunction(
                new PicklingUdfWrapper<int, string, string>(
                    (number, str) => $"{str}:{number}").Execute);

            var func2 = new PicklingWorkerFunction(
                new PicklingUdfWrapper<string, string>(
                    (str) => $"outer1:{str}").Execute);

            var func3 = new PicklingWorkerFunction(
                new PicklingUdfWrapper<string, string>(
                    (str) => $"outer2:{str}").Execute);

            object[] input = { 100, "name" };

            // Validate one-level chaining.
            PicklingWorkerFunction chainedFunc1 = PicklingWorkerFunction.Chain(func1, func2);
            Assert.Equal("outer1:name:100", chainedFunc1.Func(0, input, new[] { 0, 1 }));

            // Validate two-level chaining.
            PicklingWorkerFunction chainedFunc2 = PicklingWorkerFunction.Chain(chainedFunc1, func3);
            Assert.Equal("outer2:outer1:name:100", chainedFunc2.Func(0, input, new[] { 0, 1 }));
        }

        [Fact]
        public void TestInvalidChainingPickling()
        {
            var func1 = new PicklingWorkerFunction(
                new PicklingUdfWrapper<int, string, string>(
                    (number, str) => $"{str}:{number}").Execute);

            var func2 = new PicklingWorkerFunction(
                new PicklingUdfWrapper<string, string>(
                    (str) => $"outer1:{str}").Execute);

            object[] input = { 100, "name" };

            // The order does not align since workerFunction2 is executed first.
            PicklingWorkerFunction chainedFunc1 = PicklingWorkerFunction.Chain(func2, func1);
            Assert.ThrowsAny<Exception>(() => chainedFunc1.Func(0, input, new[] { 0, 1 }));
        }

        [Fact]
        public void TestArrowWorkerFunction()
        {
            var func = new ArrowWorkerFunction(
                new ArrowUdfWrapper<StringArray, StringArray>(
                    (str) => str).Execute);

            string[] input = { "arg1" };
            ArrowTestUtils.AssertEquals(
                input[0],
                func.Func(new[] { ToArrowArray(input) }, new[] { 0 }));
        }

        [Fact]
        public void TestDataFrameWorkerFunction()
        {
            var func = new DataFrameWorkerFunction(
                new DataFrameUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (str) => str).Execute);

            string[] input = { "arg1" };
            var column = (StringArray)ToArrowArray(input);
            ArrowStringDataFrameColumn ArrowStringDataFrameColumn = ToArrowStringDataFrameColumn(column);
            ArrowTestUtils.AssertEquals(
                input[0],
                func.Func(new[] { ArrowStringDataFrameColumn }, new[] { 0 }));
        }

        /// <summary>
        /// Tests the ArrowWorkerFunction handles boolean types correctly
        /// for both input and output.
        /// </summary>
        [Fact]
        public void TestArrowWorkerFunctionForBool()
        {
            var func = new ArrowWorkerFunction(
                new ArrowUdfWrapper<StringArray, BooleanArray, BooleanArray>(
                    (strings, flags) => (BooleanArray)ToArrowArray(
                        Enumerable.Range(0, strings.Length)
                            .Select(i => flags.GetValue(i).Value || strings.GetString(i).Contains("true"))
                            .ToArray())).Execute);

            IArrowArray[] input = new[]
            {
                ToArrowArray(new[] { "arg1_true", "arg1_true", "arg1_false", "arg1_false" }),
                ToArrowArray(new[] { true, false, true, false }),
            };
            var results = (BooleanArray)func.Func(input, new[] { 0, 1 });
            Assert.Equal(4, results.Length);
            Assert.True(results.GetValue(0).Value);
            Assert.True(results.GetValue(1).Value);
            Assert.True(results.GetValue(2).Value);
            Assert.False(results.GetValue(3).Value);
        }

        /// <summary>
        /// Tests the DataFrameWorkerFunction handles boolean types correctly
        /// for both input and output.
        /// </summary>
        [Fact]
        public void TestDataFrameWorkerFunctionForBool()
        {
            var func = new DataFrameWorkerFunction(
                new DataFrameUdfWrapper<ArrowStringDataFrameColumn, BooleanDataFrameColumn, BooleanDataFrameColumn>(
                    (strings, flags) =>
                    {
                        for (long i = 0; i < strings.Length; ++i)
                        {
                            flags[i] = flags[i].Value || strings[i].Contains("true");
                        }
                        return flags;
                    }).Execute);

            var stringColumn = (StringArray)ToArrowArray(new[] { "arg1_true", "arg1_true", "arg1_false", "arg1_false" });

            ArrowStringDataFrameColumn ArrowStringDataFrameColumn = ToArrowStringDataFrameColumn(stringColumn);
            var boolColumn = new BooleanDataFrameColumn("Bool", Enumerable.Range(0, 4).Select(x => x % 2 == 0));
            var input = new DataFrameColumn[]
                {
                    ArrowStringDataFrameColumn,
                    boolColumn
                };
            var results = (BooleanDataFrameColumn)func.Func(input, new[] { 0, 1 });
            Assert.Equal(4, results.Length);
            Assert.True(results[0]);
            Assert.True(results[1]);
            Assert.True(results[2]);
            Assert.False(results[3]);
        }

        [Fact]
        public void TestChainingArrowWorkerFunction()
        {
            var func1 = new ArrowWorkerFunction(
                new ArrowUdfWrapper<Int32Array, StringArray, StringArray>(
                    (numbers, strings) => (StringArray)ToArrowArray(
                        Enumerable.Range(0, strings.Length)
                            .Select(i => $"{strings.GetString(i)}:{numbers.Values[i]}")
                            .ToArray())).Execute);

            var func2 = new ArrowWorkerFunction(
                new ArrowUdfWrapper<StringArray, StringArray>(
                    (strings) => (StringArray)ToArrowArray(
                        Enumerable.Range(0, strings.Length)
                            .Select(i => $"outer1:{strings.GetString(i)}")
                            .ToArray())).Execute);

            var func3 = new ArrowWorkerFunction(
                new ArrowUdfWrapper<StringArray, StringArray>(
                    (strings) => (StringArray)ToArrowArray(
                        Enumerable.Range(0, strings.Length)
                            .Select(i => $"outer2:{strings.GetString(i)}")
                            .ToArray())).Execute);

            var input = new IArrowArray[]
            {
                ToArrowArray(new[] { 100 }),
                ToArrowArray(new[] { "name" })
            };

            // Validate one-level chaining.
            ArrowWorkerFunction chainedFunc1 = ArrowWorkerFunction.Chain(func1, func2);
            AssertEquals(
                "outer1:name:100",
                chainedFunc1.Func(input, new[] { 0, 1 }));

            // Validate two-level chaining.
            ArrowWorkerFunction chainedFunc2 = ArrowWorkerFunction.Chain(chainedFunc1, func3);
            AssertEquals(
                "outer2:outer1:name:100",
                chainedFunc2.Func(input, new[] { 0, 1 }));
        }

        [Fact]
        public void TestChainingDataFrameWorkerFunction()
        {
            var func1 = new DataFrameWorkerFunction(
                new DataFrameUdfWrapper<Int32DataFrameColumn, ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (numbers, strings) =>
                    {
                        long i = 0;
                        return strings.Apply(cur => $"{cur}:{numbers[i++]}");
                    }).Execute);

            var func2 = new DataFrameWorkerFunction(
                new DataFrameUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (strings) => strings.Apply(cur => $"outer1:{cur}"))
                    .Execute);

            var func3 = new DataFrameWorkerFunction(
                new DataFrameUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (strings) => strings.Apply(cur => $"outer2:{cur}"))
                    .Execute);

            string[] inputString = { "name" };
            var column = (StringArray)ToArrowArray(inputString);
            ArrowStringDataFrameColumn ArrowStringDataFrameColumn = ToArrowStringDataFrameColumn(column);
            var input = new DataFrameColumn[]
                {
                    new Int32DataFrameColumn("Int", new List<int>() { 100 }),
                    ArrowStringDataFrameColumn
                };

            // Validate one-level chaining.
            DataFrameWorkerFunction chainedFunc1 = DataFrameWorkerFunction.Chain(func1, func2);
            ArrowTestUtils.AssertEquals(
                "outer1:name:100",
                chainedFunc1.Func(input, new[] { 0, 1 }));

            // Validate two-level chaining.
            DataFrameWorkerFunction chainedFunc2 = DataFrameWorkerFunction.Chain(chainedFunc1, func3);
            ArrowTestUtils.AssertEquals(
                "outer2:outer1:name:100",
                chainedFunc2.Func(input, new[] { 0, 1 }));
        }

        [Fact]
        public void TestInvalidChainingArrow()
        {
            var func1 = new ArrowWorkerFunction(
                new ArrowUdfWrapper<Int32Array, StringArray, StringArray>(
                    (numbers, strings) => (StringArray)ToArrowArray(
                        Enumerable.Range(0, strings.Length)
                            .Select(i => $"{strings.GetString(i)}:{numbers.Values[i]}")
                            .ToArray())).Execute);

            var func2 = new ArrowWorkerFunction(
                new ArrowUdfWrapper<StringArray, StringArray>(
                    (strings) => (StringArray)ToArrowArray(
                        Enumerable.Range(0, strings.Length)
                            .Select(i => $"outer1:{strings.GetString(i)}")
                            .ToArray())).Execute);

            IArrowArray[] input = new[]
            {
                ToArrowArray(new[] { 100 }),
                ToArrowArray(new[] { "name" })
            };

            // The order does not align since workerFunction2 is executed first.
            ArrowWorkerFunction chainedFunc1 = ArrowWorkerFunction.Chain(func2, func1);
            Assert.ThrowsAny<Exception>(() => chainedFunc1.Func(input, new[] { 0, 1 }));
        }

        [Fact]
        public void TestInvalidChainingDataFrame()
        {
            var func1 = new DataFrameWorkerFunction(
                new DataFrameUdfWrapper<Int32DataFrameColumn, ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (numbers, strings) =>
                    {
                        long i = 0;
                        return strings.Apply(cur => $"{cur}:{numbers[i++]}");
                    }).Execute);

            var func2 = new DataFrameWorkerFunction(
                new DataFrameUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (strings) => strings.Apply(cur => $"outer1:{cur}"))
                    .Execute);

            string[] inputString = { "name" };
            var column = (StringArray)ToArrowArray(inputString);
            ArrowStringDataFrameColumn ArrowStringDataFrameColumn = ToArrowStringDataFrameColumn(column);
            var input = new DataFrameColumn[]
                {
                    new Int32DataFrameColumn("Int", new List<int>() { 100 }),
                    ArrowStringDataFrameColumn
                };

            // The order does not align since workerFunction2 is executed first.
            DataFrameWorkerFunction chainedFunc1 = DataFrameWorkerFunction.Chain(func2, func1);
            Assert.ThrowsAny<Exception>(() => chainedFunc1.Func(input, new[] { 0, 1 }));
        }
    }
}
