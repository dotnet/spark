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
            Assert.Equal(0x01, TypeConverter.Convert<byte>((byte)0x01));
            Assert.Equal(1, TypeConverter.Convert<sbyte>((sbyte)0x01));
            Assert.Equal(1, TypeConverter.Convert<short>((short)1));
            Assert.Equal(1, TypeConverter.Convert<ushort>((ushort)1));
            Assert.Equal(1, TypeConverter.Convert<int>(1));
            Assert.Equal(1u, TypeConverter.Convert<uint>(1u));
            Assert.Equal(1L, TypeConverter.Convert<long>(1L));
            Assert.Equal(1ul, TypeConverter.Convert<ulong>(1ul));
            Assert.Equal(1.0f, TypeConverter.Convert<float>(1.0f));
            Assert.Equal(1.0d, TypeConverter.Convert<double>(1.0d));
            Assert.Equal(1.0m, TypeConverter.Convert<decimal>(1.0m));
            Assert.Equal('a', TypeConverter.Convert<char>('a'));
            Assert.Equal("test", TypeConverter.Convert<string>("test"));
            Assert.True(TypeConverter.Convert<bool>(true));
        }

        [Fact]
        public void TestArrayList()
        {
            ArrayList expected = new ArrayList(Enumerable.Range(0, 10).ToArray());
            ArrayList actual = TypeConverter.Convert<ArrayList>(expected);
            Assert.True(ReferenceEquals(expected, actual));
        }

        [Fact]
        public void TestArray()
        {
            int[] expected = Enumerable.Range(0, 10).ToArray();
            ArrayList arrayList = new ArrayList(expected);
            int[] actual = TypeConverter.Convert<int[]>(arrayList);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestArrayArray()
        {
            int[][] expected =
                Enumerable.Range(0, 10).Select(i => Enumerable.Range(i, 10).ToArray()).ToArray();

            ArrayList arrayList = new ArrayList(expected.Length);
            for (int i = 0; i < expected.Length; ++i)
            {
                int[] innerExpected = expected[i];
                ArrayList innerArrayList = new ArrayList(innerExpected.Length);
                for (int j = 0; j < innerExpected.Length; ++j)
                {
                    innerArrayList.Add(innerExpected[j]);
                }

                arrayList.Add(innerArrayList);
            }

            int[][] actual = TypeConverter.Convert<int[][]>(arrayList);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestArrayArrayArray()
        {
            int[][][] expected = Enumerable.Range(0, 10)
                .Select(i => Enumerable.Range(i, 10))
                .Select(arr => arr.Select(j => Enumerable.Range(j, 10).ToArray()).ToArray())
                .ToArray();

            ArrayList arrayList = new ArrayList(expected.Length);
            for (int i = 0; i < expected.Length; ++i)
            {
                int[][] innerExpected = expected[i];
                ArrayList innerArrayList = new ArrayList(innerExpected.Length);
                for (int j = 0; j < innerExpected.Length; ++j)
                {
                    int[] innerInnerExpected = expected[i][j];
                    ArrayList innerInnerArrayList = new ArrayList(innerInnerExpected.Length);
                    for (int k = 0; k < innerInnerExpected.Length; ++k)
                    {
                        innerInnerArrayList.Add(innerInnerExpected[k]);
                    }

                    innerArrayList.Add(innerInnerArrayList);
                }

                arrayList.Add(innerArrayList);
            }

            int[][][] actual = TypeConverter.Convert<int[][][]>(arrayList);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestHashtable()
        {
            Hashtable expected =
                new Hashtable(Enumerable.Range(0, 10).ToDictionary(k => k, v => v * v));
            Hashtable actual = TypeConverter.Convert<Hashtable>(expected);
            Assert.True(ReferenceEquals(expected, actual));
        }

        [Fact]
        public void TestDictionary()
        {
            Dictionary<int, int> expected =
                Enumerable.Range(0, 10).ToDictionary(i => i, i => i * i);
            Hashtable hashtable = new Hashtable(expected);
            Dictionary<int, int> actual = TypeConverter.Convert<Dictionary<int, int>>(hashtable);
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

            Hashtable hashtable = new Hashtable();
            foreach (KeyValuePair<int, Dictionary<int, int>> kvp in expected)
            {
                Hashtable innerHashtable = new Hashtable();
                foreach(KeyValuePair<int, int> innerKvp in kvp.Value)
                {
                    innerHashtable[innerKvp.Key] = innerKvp.Value;
                }

                hashtable[kvp.Key] = innerHashtable;
            }

            Dictionary<int, Dictionary<int, int>> actual =
                TypeConverter.Convert<Dictionary<int, Dictionary<int, int>>>(hashtable);
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

                Hashtable hashtable = new Hashtable();
                foreach (KeyValuePair<int, int[]> kvp in expected)
                {
                    ArrayList arrayList = new ArrayList();
                    for (int i = 0; i < kvp.Value.Length; ++i)
                    {
                        arrayList.Add(kvp.Value[i]);
                    }

                    hashtable[kvp.Key] = arrayList;
                }

                Dictionary<int, int[]> actual =
                    TypeConverter.Convert<Dictionary<int, int[]>>(hashtable);
                Assert.Equal(expected, actual);
            }
            {
                Dictionary<int, int>[] expected = Enumerable
                    .Range(0, 10)
                    .Select(i => Enumerable.Range(i, 10).ToDictionary(j => j, j => j * j))
                    .ToArray();
                
                ArrayList arrayList = new ArrayList();
                for (int i = 0; i < expected.Length; ++i)
                {
                    Hashtable hashtable = new Hashtable();
                    foreach (KeyValuePair<int, int> kvp in expected[i])
                    {
                        hashtable[kvp.Key] = kvp.Value;
                    }

                    arrayList.Add(hashtable);
                }

                Dictionary<int, int>[] actual =
                    TypeConverter.Convert<Dictionary<int, int>[]>(arrayList);
                Assert.Equal(expected, actual);
            }
        }
    }
}
