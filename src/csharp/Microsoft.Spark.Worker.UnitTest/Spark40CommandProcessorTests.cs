// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Processor;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class Spark40CommandProcessorTests
    {
        [Fact]
        public void PositionalSqlBatchedUdfIsAccepted()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    CreateSerializedCommand(1)));
            long frameEnd = stream.Length;
            stream.Position = stream.Length;
            SerDe.Write(stream, 123456);
            stream.Position = 0;

            Microsoft.Spark.Worker.CommandPayload payload =
                new CommandProcessor(new Version("4.0.4")).Process(stream);

            SqlCommand command = Assert.IsType<SqlCommand>(
                Assert.Single(payload.Commands));
            Assert.Equal(new[] { 0 }, command.ArgOffsets);
            Assert.Equal(1, command.NumChainedFunctions);
            Assert.Equal(CommandSerDe.SerializedMode.Row, command.SerializerMode);
            Assert.Equal(CommandSerDe.SerializedMode.Row, command.DeserializerMode);
            var workerFunction = Assert.IsType<PicklingWorkerFunction>(
                command.WorkerFunction);
            Assert.Equal(
                6,
                workerFunction.Func(0, new object[] { 5 }, command.ArgOffsets));
            Assert.Equal(frameEnd, stream.Position);
        }

        [Fact]
        public void ChainedFunctionsUseUnaryWrapperAfterFirstFunction()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    CreateSerializedCommand(1),
                    CreateSerializedCommand(1)));

            Microsoft.Spark.Worker.CommandPayload payload =
                new CommandProcessor(new Version("4.0.4")).Process(stream);

            SqlCommand command = Assert.IsType<SqlCommand>(
                Assert.Single(payload.Commands));
            var workerFunction = Assert.IsType<PicklingWorkerFunction>(
                command.WorkerFunction);
            Assert.Equal(
                7,
                workerFunction.Func(0, new object[] { 5 }, command.ArgOffsets));
        }

        [Fact]
        public void CanonicalOffsetsAllowRepeatsAndCrossUdfBackReferences()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    new[] { 0, 0 },
                    new string[] { null, null },
                    CreateSerializedCommand(2)),
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    CreateSerializedCommand(1)));

            Microsoft.Spark.Worker.CommandPayload payload =
                new CommandProcessor(new Version("4.0.4")).Process(stream);

            Assert.Equal(2, payload.Commands.Length);
            Assert.Equal(
                new[] { 0, 0 },
                Assert.IsType<SqlCommand>(payload.Commands[0]).ArgOffsets);
            Assert.Equal(
                new[] { 0 },
                Assert.IsType<SqlCommand>(payload.Commands[1]).ArgOffsets);
        }

        [Fact]
        public void SparseFirstOffsetIsRejected()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    new[] { 1 },
                    new[] { (string)null },
                    new byte[] { 0 }));

            Assert.Throws<InvalidDataException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));
        }

        [Fact]
        public void ProfilingConsumesTheCompleteOuterFrameBeforeRejection()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: true,
                profilerName: "perf",
                new UdfFrame(
                    new[] { 0 },
                    new[] { "named" },
                    new byte[] { 0 })
                {
                    ResultId = long.MinValue
                },
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    new byte[] { 0 })
                {
                    ResultId = long.MaxValue
                });
            long frameEnd = stream.Length;
            stream.Position = stream.Length;
            SerDe.Write(stream, 123456);
            stream.Position = 0;

            NotSupportedException exception = Assert.Throws<NotSupportedException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));

            Assert.Contains("profiling", exception.Message);
            Assert.Equal(frameEnd, stream.Position);
        }

        [Fact]
        public void NamedArgumentsConsumeTheCompleteOuterFrameBeforeRejection()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    new[] { 0 },
                    new[] { "named" },
                    new byte[] { 0 }),
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    new byte[] { 0 }));
            long frameEnd = stream.Length;

            NotSupportedException exception = Assert.Throws<NotSupportedException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));

            Assert.Contains("named", exception.Message);
            Assert.Equal(frameEnd, stream.Position);
        }

        [Fact]
        public void TruncatedLaterProfilerResultIdWinsOverCapabilityRejection()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: true,
                profilerName: "memory",
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    new byte[] { 0 })
                {
                    ResultId = 42
                });
            stream.SetLength(stream.Length - 1);
            stream.Position = 0;

            Assert.Throws<EndOfStreamException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));
        }

        [Fact]
        public void FirstWrapperArityMustMatchArgumentCount()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    Array.Empty<int>(),
                    Array.Empty<string>(),
                    CreateSerializedCommand(1)));

            Assert.Throws<InvalidDataException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));
        }

        [Fact]
        public void ChainedWrapperAfterFirstMustBeUnary()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    CreateSerializedCommand(1),
                    CreateSerializedCommand(2)));

            Assert.Throws<InvalidDataException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));
        }

        [Fact]
        public void EveryCommandIsPreflightedBeforeAnyDeserialization()
        {
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    CreateSerializedCommand(1),
                    new byte[] { 0 }));

            InvalidDataException exception = Assert.Throws<InvalidDataException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));

            Assert.DoesNotContain("arity", exception.Message);
        }

        [Fact]
        public void ReplEnvelopeIsKnownButUnsupported()
        {
            byte[] command = CreateSerializedCommand(1);
            command[18] = (byte)'R';
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    command));

            Assert.Throws<NotSupportedException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));
        }

        [Fact]
        public void NestedLengthMustConsumeTheCompleteCommandBuffer()
        {
            byte[] command = CreateSerializedCommand(1);
            Array.Resize(ref command, command.Length + 1);
            using MemoryStream stream = CreateFrame(
                isProfiling: false,
                profilerName: null,
                new UdfFrame(
                    new[] { 0 },
                    new[] { (string)null },
                    command));

            Assert.Throws<InvalidDataException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(0)]
        [InlineData(1025)]
        public void UdfCountIsBoundedBeforeAllocation(int count)
        {
            using var stream = new MemoryStream();
            SerDe.Write(stream, 100);
            SerDe.Write(stream, false);
            SerDe.Write(stream, count);
            stream.Position = 0;

            Assert.Throws<InvalidDataException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));
        }

        [Theory]
        [InlineData(2)]
        [InlineData(255)]
        public void BooleanFieldsOnlyAcceptZeroOrOne(byte value)
        {
            using var stream = new MemoryStream();
            SerDe.Write(stream, 100);
            SerDe.Write(stream, value);
            stream.Position = 0;

            Assert.Throws<InvalidDataException>(() =>
                new CommandProcessor(new Version("4.0.4")).Process(stream));
        }

        private static byte[] CreateSerializedCommand(int arity)
        {
            PicklingWorkerFunction.ExecuteDelegate udf = arity switch
            {
                0 => new PicklingUdfWrapper<int>(() => 1).Execute,
                1 => new PicklingUdfWrapper<int, int>((value) => value + 1).Execute,
                2 => new PicklingUdfWrapper<int, int, int>(
                    (left, right) => left + right).Execute,
                _ => throw new ArgumentOutOfRangeException(nameof(arity))
            };

            return CommandSerDe.Serialize(
                udf,
                CommandSerDe.SerializedMode.Row,
                CommandSerDe.SerializedMode.Row);
        }

        private static MemoryStream CreateFrame(
            bool isProfiling,
            string profilerName,
            params UdfFrame[] udfs)
        {
            var stream = new MemoryStream();
            SerDe.Write(stream, 100);
            SerDe.Write(stream, isProfiling);
            if (isProfiling)
            {
                SerDe.Write(stream, profilerName);
            }

            SerDe.Write(stream, udfs.Length);
            foreach (UdfFrame udf in udfs)
            {
                SerDe.Write(stream, udf.Offsets.Length);
                for (int i = 0; i < udf.Offsets.Length; ++i)
                {
                    SerDe.Write(stream, udf.Offsets[i]);
                    string name = udf.Names[i];
                    SerDe.Write(stream, name != null);
                    if (name != null)
                    {
                        SerDe.Write(stream, name);
                    }
                }

                SerDe.Write(stream, udf.Commands.Length);
                foreach (byte[] command in udf.Commands)
                {
                    SerDe.Write(stream, command.Length);
                    SerDe.Write(stream, command);
                }

                if (isProfiling)
                {
                    SerDe.Write(stream, udf.ResultId);
                }
            }

            stream.Position = 0;
            return stream;
        }

        private sealed class UdfFrame
        {
            internal UdfFrame(
                int[] offsets,
                string[] names,
                params byte[][] commands)
            {
                Offsets = offsets;
                Names = names;
                Commands = commands;
            }

            internal int[] Offsets { get; }

            internal string[] Names { get; }

            internal byte[][] Commands { get; }

            internal long ResultId { get; set; }
        }
    }
}
