// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Processor;
using Xunit;
using Array = System.Array;

namespace Microsoft.Spark.Worker.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class Spark40ArrowCommandProcessorTests
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void ScalarConsumesConfigurationAndNameFlagsBeforeArrow(bool dataFrame)
        {
            using MemoryStream stream = Frame(200, new[] { 0 }, new[] { Scalar(dataFrame) },
                configuration: new[] { Pair("spark.sql.session.timeZone", "America/Los_Angeles") });
            long frameEnd = stream.Length;
            AppendSentinel(stream);

            SqlCommand command = ReadSingle(stream);
            Assert.Equal(new[] { 0 }, command.ArgOffsets);
            Assert.Null(command.ReturnSchema);
            Assert.Equal("America/Los_Angeles",
                command.ArrowConfiguration["spark.sql.session.timeZone"]);
            Assert.Equal(dataFrame ? typeof(DataFrameWorkerFunction) : typeof(ArrowWorkerFunction),
                command.WorkerFunction.GetType());
            Assert.Equal(frameEnd, stream.Position);
            Assert.Equal(123456, SerDe.ReadInt32(stream));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GroupedMapDecodesComputedKeyWithoutNameFlags(bool dataFrame)
        {
            using MemoryStream stream = Frame(201, new[] { 4, 1, 0, 1, 2 },
                new[] { Grouped(dataFrame) });
            long frameEnd = stream.Length;
            AppendSentinel(stream);

            SqlCommand command = ReadSingle(stream);
            Assert.Equal(new[] { 0 }, command.GroupingKeyOffsets);
            Assert.Equal(new[] { 1, 2 }, command.ArgOffsets);
            Assert.Equal(Schema().Json, command.ReturnSchema.Json);
            Assert.Equal(dataFrame ? typeof(DataFrameGroupedMapWorkerFunction) :
                typeof(ArrowGroupedMapWorkerFunction), command.WorkerFunction.GetType());
            Assert.Equal(frameEnd, stream.Position);
            Assert.Equal(123456, SerDe.ReadInt32(stream));
        }

        [Theory]
        [InlineData(0, 0)]
        [InlineData(0, 20)]
        [InlineData(2, 20)]
        public void GroupedMetadataAllowsZeroKeysEmptyValuesWideTablesAndRepeatedKeys(
            int keyCount, int valueCount)
        {
            int[] metadata = new[] { 1 + keyCount + valueCount, keyCount }
                .Concat(Enumerable.Repeat(0, keyCount))
                .Concat(Enumerable.Range(0, valueCount)).ToArray();
            using MemoryStream stream = Frame(201, metadata, new[] { Grouped(false) });
            SqlCommand command = ReadSingle(stream);
            Assert.Equal(Enumerable.Repeat(0, keyCount), command.GroupingKeyOffsets);
            Assert.Equal(Enumerable.Range(0, valueCount), command.ArgOffsets);
        }

        [Theory]
        [InlineData(new[] { 3, 1, 0, 0, 1 })]
        [InlineData(new[] { 4, 4, 0, 0, 1 })]
        [InlineData(new[] { 4, -1, 0, 0, 1 })]
        [InlineData(new[] { 2, 0, -1 })]
        [InlineData(new[] { 0 })]
        public void MalformedGroupedOffsetsAreRejected(int[] offsets)
        {
            using MemoryStream stream = Frame(201, offsets, new[] { Grouped(false) });
            Assert.Throws<InvalidDataException>(() => ReadSingle(stream));
        }

        [Fact]
        public void GroupedMapCannotChainFunctions()
        {
            using MemoryStream stream = Frame(201, new[] { 1, 0 },
                new[] { Grouped(false), Grouped(false) });
            Assert.Throws<InvalidDataException>(() => ReadSingle(stream));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void ScalarSameRepresentationChainIsAccepted(bool dataFrame)
        {
            using MemoryStream stream = Frame(200, new[] { 0 },
                new[] { Scalar(dataFrame), Scalar(dataFrame) });
            Assert.Equal(2, ReadSingle(stream).NumChainedFunctions);
        }

        [Fact]
        public void MixedRepresentationChainIsRejected()
        {
            using MemoryStream stream = Frame(200, new[] { 0 },
                new[] { Scalar(false), Scalar(true) });
            Assert.Contains("mix", Assert.Throws<NotSupportedException>(
                () => ReadSingle(stream)).Message);
        }

        [Theory]
        [InlineData(true, false)]
        [InlineData(false, true)]
        public void ValidUnsupportedFeaturesConsumeFrameBeforeRejection(bool profiling, bool named)
        {
            using MemoryStream stream = Frame(200, new[] { 0 }, new[] { Scalar(false) },
                profiling: profiling, named: named);
            long end = stream.Length;
            AppendSentinel(stream);
            Assert.Throws<NotSupportedException>(() => ReadSingle(stream));
            Assert.Equal(end, stream.Position);
            Assert.Equal(123456, SerDe.ReadInt32(stream));
        }

        [Theory]
        [InlineData(true, false)]
        [InlineData(false, true)]
        public void MalformedCommandWinsOverUnsupportedFeatures(bool profiling, bool named)
        {
            using MemoryStream stream = Frame(200, new[] { 0 },
                new[] { Scalar(false), new byte[] { 0 } }, profiling: profiling, named: named);
            Assert.Throws<InvalidDataException>(() => ReadSingle(stream));
        }

        [Fact]
        public void LaterMalformedCommandIsRejectedBeforeFirstUserTypeResolution()
        {
            byte[] first = Scalar(false);
            byte[] className = Encoding.UTF8.GetBytes(nameof(Spark40ArrowCommandProcessorTests));
            bool poisoned = false;
            for (int i = 0; i <= first.Length - className.Length; ++i)
            {
                if (first.Skip(i).Take(className.Length).SequenceEqual(className))
                {
                    first[i] = (byte)'X';
                    poisoned = true;
                }
            }

            Assert.True(poisoned);
            using MemoryStream stream = Frame(200, new[] { 0 }, new[] { first, new byte[] { 0 } });
            Assert.Contains("envelope", Assert.Throws<InvalidDataException>(
                () => ReadSingle(stream)).Message);
        }

        [Fact]
        public void ArrowLargeVariableTypesAreExplicitlyUnsupported()
        {
            using MemoryStream stream = Frame(200, new[] { 0 }, new[] { Scalar(false) },
                configuration: new[] { Pair("spark.sql.execution.arrow.useLargeVarTypes", "true") });
            long end = stream.Length;
            Assert.Contains("large", Assert.Throws<NotSupportedException>(
                () => ReadSingle(stream)).Message);
            Assert.Equal(end, stream.Position);
        }

        [Fact]
        public void ConfigurationIsTaskLocal()
        {
            var processor = new CommandProcessor(new Version("4.0.4"));
            using MemoryStream first = Frame(200, new[] { 0 }, new[] { Scalar(false) },
                configuration: new[] { Pair("spark.sql.session.timeZone", "UTC") });
            using MemoryStream second = Frame(200, new[] { 0 }, new[] { Scalar(false) });
            var firstCommand = (SqlCommand)processor.Process(first).Commands[0];
            var secondCommand = (SqlCommand)processor.Process(second).Commands[0];
            Assert.Single(firstCommand.ArrowConfiguration);
            Assert.Empty(secondCommand.ArrowConfiguration);
        }

        [Theory]
        [InlineData("spark.sql.execution.arrow.useLargeVarTypes")]
        [InlineData("spark.sql.execution.pandas.convertToArrowArraySafely")]
        [InlineData("spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName")]
        public void InvalidBooleanConfigurationIsRejected(string key)
        {
            using MemoryStream stream = Frame(200, new[] { 0 }, new[] { Scalar(false) },
                configuration: new[] { Pair(key, "not-a-boolean") });
            Assert.Throws<InvalidDataException>(() => ReadSingle(stream));
        }

        [Fact]
        public void DuplicateConfigurationIsRejected()
        {
            using MemoryStream stream = Frame(200, new[] { 0 }, new[] { Scalar(false) },
                configuration: new[] { Pair("key", "one"), Pair("key", "two") });
            Assert.Throws<InvalidDataException>(() => ReadSingle(stream));
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(65)]
        public void ConfigurationCountIsBoundedBeforeAllocation(int count)
        {
            using var stream = new MemoryStream();
            SerDe.Write(stream, 200);
            SerDe.Write(stream, count);
            stream.Position = 0;
            Assert.Throws<InvalidDataException>(() => ReadSingle(stream));
        }

        [Fact]
        public void FragmentedReadsAndTruncatedCommandAreHandledExactly()
        {
            using MemoryStream frame = Frame(201, new[] { 2, 0, 0 }, new[] { Grouped(false) });
            using var fragmented = new FragmentedStream(frame.ToArray());
            Assert.Single(ReadSingle(fragmented).ArgOffsets);
            byte[] bytes = frame.ToArray();
            foreach (int length in new[] { 0, 3, 6, 8, 12, bytes.Length - 1 })
            {
                using var truncated = new FragmentedStream(bytes.Take(length).ToArray());
                Assert.Throws<EndOfStreamException>(() => ReadSingle(truncated));
            }
        }

        [Fact]
        public void InvalidUtf8ConfigurationIsRejected()
        {
            using var stream = new MemoryStream();
            SerDe.Write(stream, 200);
            SerDe.Write(stream, 1);
            SerDe.Write(stream, 1);
            stream.WriteByte(0xff);
            stream.Position = 0;
            Assert.Throws<InvalidDataException>(() => ReadSingle(stream));
        }

        private static SqlCommand ReadSingle(Stream stream) => (SqlCommand)Assert.Single(
            new CommandProcessor(new Version("4.0.4")).Process(stream).Commands);

        private static StructType Schema() => new StructType(new[]
        {
            new StructField("value", new IntegerType())
        });

        private static KeyValuePair<string, string> Pair(string key, string value) =>
            new KeyValuePair<string, string>(key, value);

        private static byte[] Scalar(bool dataFrame)
        {
            Delegate wrapper = dataFrame ?
                (Delegate)(DataFrameWorkerFunction.ExecuteDelegate)new DataFrameUdfWrapper<Int32DataFrameColumn,
                    Int32DataFrameColumn>(value => value).Execute :
                (ArrowWorkerFunction.ExecuteDelegate)new ArrowUdfWrapper<Int32Array, Int32Array>(
                    value => value).Execute;
            return CommandSerDe.Serialize(wrapper, CommandSerDe.SerializedMode.Row,
                CommandSerDe.SerializedMode.Row);
        }

        private static byte[] Grouped(bool dataFrame)
        {
            Delegate wrapper = dataFrame ?
                (Delegate)(DataFrameGroupedMapWorkerFunction.ExecuteDelegate)
                    new DataFrameGroupedMapUdfWrapper(value => value).Execute :
                (ArrowGroupedMapWorkerFunction.ExecuteDelegate)
                    new ArrowGroupedMapUdfWrapper(value => value).Execute;
            return CommandSerDe.SerializeSpark40GroupedMap(wrapper, Schema());
        }

        private static MemoryStream Frame(int eval, int[] offsets, byte[][] commands,
            KeyValuePair<string, string>[] configuration = null, bool profiling = false, bool named = false)
        {
            var stream = new MemoryStream();
            SerDe.Write(stream, eval);
            configuration ??= Array.Empty<KeyValuePair<string, string>>();
            SerDe.Write(stream, configuration.Length);
            foreach (KeyValuePair<string, string> pair in configuration)
            {
                SerDe.Write(stream, pair.Key);
                SerDe.Write(stream, pair.Value);
            }

            SerDe.Write(stream, profiling);
            if (profiling)
            {
                SerDe.Write(stream, "perf");
            }

            SerDe.Write(stream, 1);
            SerDe.Write(stream, offsets.Length);
            foreach (int offset in offsets)
            {
                SerDe.Write(stream, offset);
                if (eval == 200)
                {
                    SerDe.Write(stream, named);
                    if (named)
                    {
                        SerDe.Write(stream, "value");
                    }
                }
            }

            SerDe.Write(stream, commands.Length);
            foreach (byte[] command in commands)
            {
                SerDe.Write(stream, command.Length);
                stream.Write(command, 0, command.Length);
            }

            if (profiling)
            {
                SerDe.Write(stream, 42L);
            }

            stream.Position = 0;
            return stream;
        }

        private static void AppendSentinel(MemoryStream stream)
        {
            stream.Position = stream.Length;
            SerDe.Write(stream, 123456);
            stream.Position = 0;
        }

        private sealed class FragmentedStream : MemoryStream
        {
            internal FragmentedStream(byte[] bytes) : base(bytes) { }

            public override int Read(byte[] buffer, int offset, int count) =>
                base.Read(buffer, offset, Math.Min(1, count));
        }
    }
}
