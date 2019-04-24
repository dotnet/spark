// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class SerDeTests
    {
        [Fact]
        public void TestReadAndWrite()
        {
            using (var ms = new MemoryStream())
            {
                // Test bool.
                SerDe.Write(ms, true);
                ms.Seek(0, SeekOrigin.Begin);
                Assert.True(SerDe.ReadBool(ms));
                ms.Seek(0, SeekOrigin.Begin);

                SerDe.Write(ms, false);
                ms.Seek(0, SeekOrigin.Begin);
                Assert.False(SerDe.ReadBool(ms));
                ms.Seek(0, SeekOrigin.Begin);

                // Test int.
                SerDe.Write(ms, 12345);
                ms.Seek(0, SeekOrigin.Begin);
                Assert.Equal(12345, SerDe.ReadInt32(ms));
                ms.Seek(0, SeekOrigin.Begin);

                // Test long.
                SerDe.Write(ms, 123456789000);
                ms.Seek(0, SeekOrigin.Begin);
                Assert.Equal(123456789000, SerDe.ReadInt64(ms));
                ms.Seek(0, SeekOrigin.Begin);

                // Test double.
                SerDe.Write(ms, Math.PI);
                ms.Seek(0, SeekOrigin.Begin);
                Assert.Equal(Math.PI, SerDe.ReadDouble(ms));
                ms.Seek(0, SeekOrigin.Begin);

                // Test string.
                SerDe.Write(ms, "hello world!");
                ms.Seek(0, SeekOrigin.Begin);
                Assert.Equal("hello world!", SerDe.ReadString(ms));
                ms.Seek(0, SeekOrigin.Begin);
            }
        }

        [Fact]
        public void TestReadBytes()
        {
            // Test the case where invalid length is given.
            Assert.Throws<ArgumentOutOfRangeException>(
                () => SerDe.ReadBytes(new MemoryStream(), -1));

            // Test reading null length. 
            var ms = new MemoryStream();
            SerDe.Write(ms, (int)SpecialLengths.NULL);
            ms.Seek(0, SeekOrigin.Begin);
            Assert.Null(SerDe.ReadBytes(ms));
        }
    }
}
