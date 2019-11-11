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
            var chainedFunc1 = PicklingWorkerFunction.Chain(func1, func2);
            Assert.Equal("outer1:name:100", chainedFunc1.Func(0, input, new[] { 0, 1 }));

            // Validate two-level chaining.
            var chainedFunc2 = PicklingWorkerFunction.Chain(chainedFunc1, func3);
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
            var chainedFunc1 = PicklingWorkerFunction.Chain(func2, func1);
            Assert.ThrowsAny<Exception>(() => chainedFunc1.Func(0, input, new[] { 0, 1 }));
        }

        [Fact]
        public void TestArrowWorkerFunction()
        {
            var func = new ArrowWorkerFunction(
                new ArrowUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (str) => str).Execute);

            string[] input = { "arg1" };
            StringArray column = (StringArray)ToArrowArray(input);
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
                new ArrowUdfWrapper<ArrowStringDataFrameColumn, PrimitiveDataFrameColumn<bool>, PrimitiveDataFrameColumn<bool>>(
                    (strings, flags) =>
                    {
                        for (long i = 0; i < strings.Length; i++)
                        {
                            flags[i] = flags[i].Value || strings[i].Contains("true");
                        }
                        return flags;
                    }).Execute);

            StringArray stringColumn = (StringArray)ToArrowArray(new[] { "arg1_true", "arg1_true", "arg1_false", "arg1_false" });

            ArrowStringDataFrameColumn ArrowStringDataFrameColumn = ToArrowStringDataFrameColumn(stringColumn);
            PrimitiveDataFrameColumn<bool> boolColumn = new PrimitiveDataFrameColumn<bool>("Bool", Enumerable.Range(0, 4).Select(x => x % 2 == 0 ? true : false));
            DataFrameColumn[] input = new DataFrameColumn[]
            {
                ArrowStringDataFrameColumn,
                boolColumn
            };
            var results = (PrimitiveDataFrameColumn<bool>)func.Func(input, new[] { 0, 1 });
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
                new ArrowUdfWrapper<PrimitiveDataFrameColumn<int>, ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (numbers, strings) =>
                    {
                        StringArray stringColumn = (StringArray)ToArrowArray(
                         Enumerable.Range(0, (int)strings.Length)
                             .Select(i => $"{strings[i]}:{numbers[i]}")
                             .ToArray());
                        return ToArrowStringDataFrameColumn(stringColumn);
                    }).Execute);

            var func2 = new ArrowWorkerFunction(
                new ArrowUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (strings) =>
                    {
                        StringArray stringColumn = (StringArray)ToArrowArray(
                        Enumerable.Range(0, (int)strings.Length)
                            .Select(i => $"outer1:{strings[i]}")
                            .ToArray());
                        return ToArrowStringDataFrameColumn(stringColumn);
                    }).Execute);


            var func3 = new ArrowWorkerFunction(
                new ArrowUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (strings) =>
                    {
                        StringArray stringColumn = (StringArray)ToArrowArray(
                         Enumerable.Range(0, (int)strings.Length)
                             .Select(i => $"outer2:{strings[(i)]}")
                             .ToArray());
                        return ToArrowStringDataFrameColumn(stringColumn);
                    }).Execute);

            string[] inputString = { "name" };
            StringArray column = (StringArray)ToArrowArray(inputString);
            ArrowStringDataFrameColumn ArrowStringDataFrameColumn = ToArrowStringDataFrameColumn(column);
            DataFrameColumn[] input = new DataFrameColumn[]
            {
                new PrimitiveDataFrameColumn<int>("Int", new List<int>() {100 }),
                ArrowStringDataFrameColumn
            };

            // Validate one-level chaining.
            var chainedFunc1 = ArrowWorkerFunction.Chain(func1, func2);
            ArrowTestUtils.AssertEquals(
                "outer1:name:100",
                chainedFunc1.Func(input, new[] { 0, 1 }));

            // Validate two-level chaining.
            var chainedFunc2 = ArrowWorkerFunction.Chain(chainedFunc1, func3);
            ArrowTestUtils.AssertEquals(
                "outer2:outer1:name:100",
                chainedFunc2.Func(input, new[] { 0, 1 }));

        }

        [Fact]
        public void TestInvalidChainingArrow()
        {
            var func1 = new ArrowWorkerFunction(
                new ArrowUdfWrapper<PrimitiveDataFrameColumn<int>, ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (numbers, strings) =>
                    {
                        StringArray stringArray = (StringArray)ToArrowArray(
                            Enumerable.Range(0, (int)strings.Length)
                                .Select(i => $"{strings[i]}:{numbers[i]}")
                                .ToArray());
                        return ToArrowStringDataFrameColumn(stringArray);
                    }).Execute);

            var func2 = new ArrowWorkerFunction(
                new ArrowUdfWrapper<ArrowStringDataFrameColumn, ArrowStringDataFrameColumn>(
                    (strings) =>
                    {
                        StringArray stringArray = (StringArray)ToArrowArray(
                         Enumerable.Range(0, (int)strings.Length)
                             .Select(i => $"outer1:{strings[i]}")
                             .ToArray());
                        return ToArrowStringDataFrameColumn(stringArray);
                    }).Execute);

            string[] inputString = { "name" };
            StringArray column = (StringArray)ToArrowArray(inputString);
            ArrowStringDataFrameColumn ArrowStringDataFrameColumn = ToArrowStringDataFrameColumn(column);
            DataFrameColumn[] input = new DataFrameColumn[]
            {
                new PrimitiveDataFrameColumn<int>("Int", new List<int>() {100 }),
                ArrowStringDataFrameColumn
            };

            // The order does not align since workerFunction2 is executed first.
            var chainedFunc1 = ArrowWorkerFunction.Chain(func2, func1);
            Assert.ThrowsAny<Exception>(() => chainedFunc1.Func(input, new[] { 0, 1 }));
        }
    }
}
