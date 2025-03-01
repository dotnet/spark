// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class TypeConverterTests
    {
        [Fact]
        public void TestBaseCase()
        {
            Assert.Equal((byte)0x01, TypeConverter.ConvertTo<byte>((byte)0x01));
            Assert.Equal((sbyte)0x01, TypeConverter.ConvertTo<sbyte>((sbyte)0x01));
            Assert.Equal((short)1, TypeConverter.ConvertTo<short>((short)1));
            Assert.Equal((ushort)1, TypeConverter.ConvertTo<ushort>((ushort)1));
            Assert.Equal(1, TypeConverter.ConvertTo<int>(1));
            Assert.Equal(1L, TypeConverter.ConvertTo<long>(1));
            Assert.Equal(1u, TypeConverter.ConvertTo<uint>(1u));
            Assert.Equal(1L, TypeConverter.ConvertTo<long>(1L));
            Assert.Equal(1ul, TypeConverter.ConvertTo<ulong>(1ul));
            Assert.Equal(1.0f, TypeConverter.ConvertTo<float>(1.0f));
            Assert.Equal(1.0d, TypeConverter.ConvertTo<double>(1.0d));
            Assert.Equal(1.0m, TypeConverter.ConvertTo<decimal>(1.0m));
            Assert.Equal('a', TypeConverter.ConvertTo<char>('a'));
            Assert.Equal("test", TypeConverter.ConvertTo<string>("test"));
            Assert.True(TypeConverter.ConvertTo<bool>(true));
        }

        [Fact]
        public void TestArrayList()
        {
            var expected = new ArrayList(Enumerable.Range(0, 10).ToArray());
            ArrayList actual = TypeConverter.ConvertTo<ArrayList>(expected);
            Assert.Same(expected, actual);
        }

        [Fact]
        public void TestArray()
        {
            int[] expected = Enumerable.Range(0, 10).ToArray();
            var arrayList = new ArrayList(expected);
            int[] actual = TypeConverter.ConvertTo<int[]>(arrayList);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestArrayArray()
        {
            int[][] expected =
                Enumerable.Range(0, 10).Select(i => Enumerable.Range(i, 10).ToArray()).ToArray();

            var arrayList = new ArrayList(expected.Length);
            for (int i = 0; i < expected.Length; ++i)
            {
                int[] innerExpected = expected[i];
                var innerArrayList = new ArrayList(innerExpected.Length);
                for (int j = 0; j < innerExpected.Length; ++j)
                {
                    innerArrayList.Add(innerExpected[j]);
                }

                arrayList.Add(innerArrayList);
            }

            int[][] actual = TypeConverter.ConvertTo<int[][]>(arrayList);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestArrayArrayArray()
        {
            int[][][] expected = Enumerable.Range(0, 10)
                .Select(i => Enumerable.Range(i, 10))
                .Select(arr => arr.Select(j => Enumerable.Range(j, 10).ToArray()).ToArray())
                .ToArray();

            var arrayList = new ArrayList(expected.Length);
            for (int i = 0; i < expected.Length; ++i)
            {
                int[][] innerExpected = expected[i];
                var innerArrayList = new ArrayList(innerExpected.Length);
                for (int j = 0; j < innerExpected.Length; ++j)
                {
                    int[] innerInnerExpected = expected[i][j];
                    var innerInnerArrayList = new ArrayList(innerInnerExpected.Length);
                    for (int k = 0; k < innerInnerExpected.Length; ++k)
                    {
                        innerInnerArrayList.Add(innerInnerExpected[k]);
                    }

                    innerArrayList.Add(innerInnerArrayList);
                }

                arrayList.Add(innerArrayList);
            }

            int[][][] actual = TypeConverter.ConvertTo<int[][][]>(arrayList);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestHashtable()
        {
            var expected =
                new Hashtable(Enumerable.Range(0, 10).ToDictionary(k => k, v => v * v));
            Hashtable actual = TypeConverter.ConvertTo<Hashtable>(expected);
            Assert.Same(expected, actual);
        }

        [Fact]
        public void TestDictionary()
        {
            Dictionary<int, int> expected =
                Enumerable.Range(0, 10).ToDictionary(i => i, i => i * i);
            var hashtable = new Hashtable(expected);
            Dictionary<int, int> actual = TypeConverter.ConvertTo<Dictionary<int, int>>(hashtable);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestDictionaryDictionary()
        {
            Dictionary<int, Dictionary<int, int>> expected = Enumerable
                .Range(0, 10)
                .ToDictionary(
                    i => i,
                    i => Enumerable.Range(i, 10).ToDictionary(j => j, j => j * j));

            var hashtable = new Hashtable();
            foreach (KeyValuePair<int, Dictionary<int, int>> kvp in expected)
            {
                var innerHashtable = new Hashtable();
                foreach(KeyValuePair<int, int> innerKvp in kvp.Value)
                {
                    innerHashtable[innerKvp.Key] = innerKvp.Value;
                }

                hashtable[kvp.Key] = innerHashtable;
            }

            Dictionary<int, Dictionary<int, int>> actual =
                TypeConverter.ConvertTo<Dictionary<int, Dictionary<int, int>>>(hashtable);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestDictionaryAndArray()
        {
            {
                Dictionary<int, int[]> expected = Enumerable
                    .Range(0, 10)
                    .ToDictionary(
                        i => i,
                        i => Enumerable.Range(i, 10).ToArray());

                var hashtable = new Hashtable();
                foreach (KeyValuePair<int, int[]> kvp in expected)
                {
                    var arrayList = new ArrayList();
                    for (int i = 0; i < kvp.Value.Length; ++i)
                    {
                        arrayList.Add(kvp.Value[i]);
                    }

                    hashtable[kvp.Key] = arrayList;
                }

                Dictionary<int, int[]> actual =
                    TypeConverter.ConvertTo<Dictionary<int, int[]>>(hashtable);
                Assert.Equal(expected, actual);
            }
            {
                Dictionary<int, int>[] expected = Enumerable
                    .Range(0, 10)
                    .Select(i => Enumerable.Range(i, 10).ToDictionary(j => j, j => j * j))
                    .ToArray();
                
                var arrayList = new ArrayList();
                for (int i = 0; i < expected.Length; ++i)
                {
                    var hashtable = new Hashtable();
                    foreach (KeyValuePair<int, int> kvp in expected[i])
                    {
                        hashtable[kvp.Key] = kvp.Value;
                    }

                    arrayList.Add(hashtable);
                }

                Dictionary<int, int>[] actual =
                    TypeConverter.ConvertTo<Dictionary<int, int>[]>(arrayList);
                Assert.Equal(expected, actual);
            }
        }
    }
}
