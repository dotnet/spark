// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Processor;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class CommandProcessorTests
    {
        [Fact]
        public void Spark40EvalTypeGateAllowsOnlySqlBatchedUdf()
        {
            using MemoryStream stream = CreateEvalTypeStream(100);

            UdfUtils.PythonEvalType evalType =
                new CommandProcessor(new Version("4.0.4")).ReadEvalType(stream);

            Assert.Equal(UdfUtils.PythonEvalType.SQL_BATCHED_UDF, evalType);
            Assert.Equal(sizeof(int), stream.Position);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(101)]
        [InlineData(200)]
        [InlineData(201)]
        [InlineData(202)]
        [InlineData(203)]
        [InlineData(204)]
        [InlineData(205)]
        [InlineData(206)]
        [InlineData(207)]
        [InlineData(208)]
        [InlineData(209)]
        [InlineData(210)]
        [InlineData(211)]
        [InlineData(212)]
        [InlineData(300)]
        [InlineData(301)]
        public void Spark40EvalTypeGateRejectsKnownUnsupportedTypes(int rawEvalType)
        {
            using MemoryStream stream = CreateEvalTypeStream(rawEvalType);

            Assert.Throws<NotSupportedException>(() =>
                new CommandProcessor(new Version("4.0.4")).ReadEvalType(stream));
            Assert.Equal(sizeof(int), stream.Position);
        }

        [Theory]
        [InlineData(int.MinValue)]
        [InlineData(-1)]
        [InlineData(1)]
        [InlineData(99)]
        [InlineData(102)]
        [InlineData(199)]
        [InlineData(213)]
        [InlineData(299)]
        [InlineData(302)]
        [InlineData(int.MaxValue)]
        public void Spark40EvalTypeGateRejectsUnknownTypes(int rawEvalType)
        {
            using MemoryStream stream = CreateEvalTypeStream(rawEvalType);

            Assert.Throws<InvalidDataException>(() =>
                new CommandProcessor(new Version("4.0.4")).ReadEvalType(stream));
            Assert.Equal(sizeof(int), stream.Position);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public void Spark40EvalTypeGateRequiresExactlyFourBytes(int length)
        {
            using var stream = new MemoryStream(new byte[length]);

            Assert.Throws<EndOfStreamException>(() =>
                new CommandProcessor(new Version("4.0.4")).ReadEvalType(stream));
            Assert.Equal(length, stream.Position);
        }

        [Theory]
        [InlineData("2.4.0", 0)]
        [InlineData("3.5.1", 200)]
        public void LegacyVersionsKeepExistingEvalTypePath(string version, int rawEvalType)
        {
            using MemoryStream stream = CreateEvalTypeStream(rawEvalType);

            UdfUtils.PythonEvalType evalType =
                new CommandProcessor(new Version(version)).ReadEvalType(stream);

            Assert.Equal(rawEvalType, (int)evalType);
            Assert.Equal(sizeof(int), stream.Position);
        }

        private static MemoryStream CreateEvalTypeStream(int rawEvalType)
        {
            var stream = new MemoryStream();
            SerDe.Write(stream, rawEvalType);
            SerDe.Write(stream, 123456);
            stream.Position = 0;
            return stream;
        }
    }
}
