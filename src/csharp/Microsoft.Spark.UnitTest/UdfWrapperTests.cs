// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Arrow;
using Microsoft.Spark.Sql;
using Microsoft.Spark.UnitTest.TestUtils;
using Xunit;
using static Microsoft.Spark.UnitTest.TestUtils.ArrowTestUtils;

namespace Microsoft.Spark.UnitTest
{
    public class UdfWrapperTests
    {
        [Fact]
        public void TestPicklingUdfWrapper0()
        {
            var udfWrapper = new PicklingUdfWrapper<int>(() => 10);
            Assert.Equal(10, udfWrapper.Execute(0, null, null));
        }

        [Fact]
        public void TestPicklingUdfWrapper1()
        {
            var udfWrapper = new PicklingUdfWrapper<string, string>(
                (str1) => str1);
            ValidatePicklingWrapper(1, udfWrapper);
        }

        [Fact]
        public void TestPicklingUdfWrapper2()
        {
            var udfWrapper = new PicklingUdfWrapper<string, string, string>(
                (str1, str2) => str1 + str2);
            ValidatePicklingWrapper(2, udfWrapper);
        }

        [Fact]
        public void TestPicklingUdfWrapper3()
        {
            var udfWrapper = new PicklingUdfWrapper<string, string, string, string>(
                (str1, str2, str3) => str1 + str2 + str3);
            ValidatePicklingWrapper(3, udfWrapper);
        }

        [Fact]
        public void TestPicklingUdfWrapper4()
        {
            var udfWrapper = new PicklingUdfWrapper<string, string, string, string, string>(
                (str1, str2, str3, str4) => str1 + str2 + str3 + str4);
            ValidatePicklingWrapper(4, udfWrapper);
        }

        [Fact]
        public void TestPicklingUdfWrapper5()
        {
            var udfWrapper = new PicklingUdfWrapper<string, string, string, string, string, string>(
                (str1, str2, str3, str4, str5) => str1 + str2 + str3 + str4 + str5);
            ValidatePicklingWrapper(5, udfWrapper);
        }

        [Fact]
        public void TestPicklingUdfWrapper6()
        {
            var udfWrapper = new PicklingUdfWrapper<
                string, string, string, string, string, string, string>(
                    (str1, str2, str3, str4, str5, str6)
                        => str1 + str2 + str3 + str4 + str5 + str6);
            ValidatePicklingWrapper(6, udfWrapper);
        }

        [Fact]
        public void TestPicklingUdfWrapper7()
        {
            var udfWrapper = new PicklingUdfWrapper<
                string, string, string, string, string, string, string, string>(
                    (str1, str2, str3, str4, str5, str6, str7)
                        => str1 + str2 + str3 + str4 + str5 + str6 + str7);
            ValidatePicklingWrapper(7, udfWrapper);
        }

        [Fact]
        public void TestPicklingUdfWrapper8()
        {
            var udfWrapper = new PicklingUdfWrapper<
                string, string, string, string, string, string, string, string, string>(
                    (str1, str2, str3, str4, str5, str6, str7, str8)
                        => str1 + str2 + str3 + str4 + str5 + str6 + str7 + str8);
            ValidatePicklingWrapper(8, udfWrapper);
        }

        [Fact]
        public void TestPicklingUdfWrapper9()
        {
            var udfWrapper = new PicklingUdfWrapper<
                string, string, string, string, string, string, string, string, string, string>(
                    (str1, str2, str3, str4, str5, str6, str7, str8, str9)
                        => str1 + str2 + str3 + str4 + str5 + str6 + str7 + str8 + str9);
            ValidatePicklingWrapper(9, udfWrapper);
        }

        [Fact]
        public void TestPicklingUdfWrapper10()
        {
            var udfWrapper = new PicklingUdfWrapper<
                string, string, string, string, string, string, string, string, string, string, string>(
                    (str1, str2, str3, str4, str5, str6, str7, str8, str9, str10)
                        => str1 + str2 + str3 + str4 + str5 + str6 + str7 + str8 + str9 + str10);
            ValidatePicklingWrapper(10, udfWrapper);
        }

        // Validates the given udfWrapper, whose internal UDF concatenates all the input strings.
        private void ValidatePicklingWrapper(int numArgs, dynamic udfWrapper)
        {
            // Create one more input data than the given numArgs to validate
            // the indexing is working correctly inside UdfWrapper.
            var input = new List<string>();
            for (int i = 0; i < numArgs + 1; ++i)
            {
                input.Add($"arg{i}");
            }

            // First create argOffsets from 0 to numArgs.
            // For example, the numArgs was 3, the expected strings is "arg0arg1arg2"
            // where the argOffsets are created with { 0, 1, 2 }.
            Assert.Equal(
                string.Join("", input.GetRange(0, numArgs)),
                udfWrapper.Execute(0, input.ToArray(), Enumerable.Range(0, numArgs).ToArray()));

            // Create argOffsets from 1 to numArgs + 1.
            // For example, the numArgs was 3, the expected strings is "arg1arg2arg3"
            // where the argOffsets are created with { 1, 2, 3 }.
            Assert.Equal(
                string.Join("", input.GetRange(1, numArgs)),
                udfWrapper.Execute(0, input.ToArray(), Enumerable.Range(1, numArgs).ToArray()));
        }

        [Fact]
        public void TestArrowUdfWrapper1()
        {
            var udfWrapper = new ArrowUdfWrapper<StringArray, StringArray>(
                (str1) => str1);
            ValidateArrowWrapper(1, udfWrapper);
        }

        [Fact]
        public void TestArrowUdfWrapper2()
        {
            var udfWrapper = new ArrowUdfWrapper<StringArray, StringArray, StringArray>(
                (str1, str2) => Concat(str1, str2));
            ValidateArrowWrapper(2, udfWrapper);
        }

        [Fact]
        public void TestArrowUdfWrapper3()
        {
            var udfWrapper = new ArrowUdfWrapper<StringArray, StringArray, StringArray, StringArray>(
                (str1, str2, str3) => Concat(str1, str2, str3));
            ValidateArrowWrapper(3, udfWrapper);
        }

        [Fact]
        public void TestArrowUdfWrapper4()
        {
            var udfWrapper = new ArrowUdfWrapper<StringArray, StringArray, StringArray, StringArray, StringArray>(
                (str1, str2, str3, str4) => Concat(str1, str2, str3, str4));
            ValidateArrowWrapper(4, udfWrapper);
        }

        [Fact]
        public void TestArrowUdfWrapper5()
        {
            var udfWrapper = new ArrowUdfWrapper<StringArray, StringArray, StringArray, StringArray, StringArray, StringArray>(
                (str1, str2, str3, str4, str5) => Concat(str1, str2, str3, str4, str5));
            ValidateArrowWrapper(5, udfWrapper);
        }

        [Fact]
        public void TestArrowUdfWrapper6()
        {
            var udfWrapper = new ArrowUdfWrapper<
                StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray>(
                    (str1, str2, str3, str4, str5, str6)
                        => Concat(str1, str2, str3, str4, str5, str6));
            ValidateArrowWrapper(6, udfWrapper);
        }

        [Fact]
        public void TestArrowUdfWrapper7()
        {
            var udfWrapper = new ArrowUdfWrapper<
                StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray>(
                    (str1, str2, str3, str4, str5, str6, str7)
                        => Concat(str1, str2, str3, str4, str5, str6, str7));
            ValidateArrowWrapper(7, udfWrapper);
        }

        [Fact]
        public void TestArrowUdfWrapper8()
        {
            var udfWrapper = new ArrowUdfWrapper<
                StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray>(
                    (str1, str2, str3, str4, str5, str6, str7, str8)
                        => Concat(str1, str2, str3, str4, str5, str6, str7, str8));
            ValidateArrowWrapper(8, udfWrapper);
        }

        [Fact]
        public void TestArrowUdfWrapper9()
        {
            var udfWrapper = new ArrowUdfWrapper<
                StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray>(
                    (str1, str2, str3, str4, str5, str6, str7, str8, str9)
                        => Concat(str1, str2, str3, str4, str5, str6, str7, str8, str9));
            ValidateArrowWrapper(9, udfWrapper);
        }

        [Fact]
        public void TestArrowUdfWrapper10()
        {
            var udfWrapper = new ArrowUdfWrapper<
                StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray>(
                    (str1, str2, str3, str4, str5, str6, str7, str8, str9, str10)
                        => Concat(str1, str2, str3, str4, str5, str6, str7, str8, str9, str10));
            ValidateArrowWrapper(10, udfWrapper);
        }

        private static StringArray Concat(params StringArray[] arrays)
        {
            StringBuilder builder = new StringBuilder();
            int length = arrays[0].Length;
            string[] resultStrings = new string[length];

            for (int i = 0; i < length; i++)
            {
                foreach (StringArray array in arrays)
                {
                    builder.Append(array.GetString(i));
                }

                resultStrings[i] = builder.ToString();
                builder.Clear();
            }

            return (StringArray)ToArrowArray(resultStrings);
        }

        // Validates the given udfWrapper, whose internal UDF concatenates all the input strings.
        private void ValidateArrowWrapper(int numArgs, dynamic udfWrapper)
        {
            // Create one more input data than the given numArgs to validate
            // the indexing is working correctly inside ArrowUdfWrapper.
            var input = new IArrowArray[numArgs + 1];
            var inputStrings = new List<string>();
            for (int i = 0; i < input.Length; ++i)
            {
                inputStrings.Add($"arg{i}");
                input[i] = ToArrowArray(new string[] { $"arg{i}" });
            }

            // First create argOffsets from 0 to numArgs.
            // For example, the numArgs was 3, the expected strings is "arg0arg1arg2"
            // where the argOffsets are created with { 0, 1, 2 }.
            ArrowTestUtils.AssertEquals(
                string.Join("", inputStrings.GetRange(0, numArgs)),
                udfWrapper.Execute(input, Enumerable.Range(0, numArgs).ToArray()));

            // Create argOffsets from 1 to numArgs + 1.
            // For example, the numArgs was 3, the expected strings is "arg1arg2arg3"
            // where the argOffsets are created with { 1, 2, 3 }.
            ArrowTestUtils.AssertEquals(
                string.Join("", inputStrings.GetRange(1, numArgs)),
                udfWrapper.Execute(input, Enumerable.Range(1, numArgs).ToArray()));
        }
    }
}
