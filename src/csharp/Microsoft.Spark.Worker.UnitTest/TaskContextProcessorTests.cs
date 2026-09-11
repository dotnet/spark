// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Worker.Processor;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    public class TaskContextProcessorTests
    {
        [Fact]
        public void Spark40UsesSpark33TaskContextProtocol()
        {
            var expected = new TaskContext
            {
                IsBarrier = true,
                Port = 9999,
                Secret = "secret",
                StageId = 1,
                PartitionId = 2,
                AttemptNumber = 3,
                AttemptId = 4,
                CPUs = 5
            };
            expected.LocalProperties.Add("key", "value");

            using var stream = new MemoryStream();
            new TaskContextWriterV3_3_X().Write(stream, expected);
            stream.Position = 0;

            TaskContext actual =
                new TaskContextProcessor(new Version("4.0.4")).Process(stream);

            Assert.Equal(expected.IsBarrier, actual.IsBarrier);
            Assert.Equal(expected.Port, actual.Port);
            Assert.Equal(expected.Secret, actual.Secret);
            Assert.Equal(expected.StageId, actual.StageId);
            Assert.Equal(expected.PartitionId, actual.PartitionId);
            Assert.Equal(expected.AttemptNumber, actual.AttemptNumber);
            Assert.Equal(expected.AttemptId, actual.AttemptId);
            Assert.Equal(expected.CPUs, actual.CPUs);
            Assert.Equal(expected.LocalProperties, actual.LocalProperties);
            Assert.Equal(stream.Length, stream.Position);
        }

        [Fact]
        public void Spark41IsRejectedWithoutReadingTaskContext()
        {
            using var stream = new MemoryStream(new byte[] { 1 });

            Assert.Throws<NotSupportedException>(() =>
                new TaskContextProcessor(new Version("4.1.0")).Process(stream));
            Assert.Equal(0, stream.Position);
        }

        [Theory]
        [InlineData("2.0.0")]
        [InlineData("2.3.0")]
        [InlineData("2.4.0")]
        [InlineData("2.4.8")]
        public void Spark2IsRejectedBeforeReadingTaskContext(string version)
        {
            using var stream = new MemoryStream(new byte[16]);

            Assert.Throws<NotSupportedException>(() =>
                new TaskContextProcessor(new Version(version)).Process(stream));

            Assert.Equal(0, stream.Position);
        }

        [Theory]
        [InlineData("3.0.0", false)]
        [InlineData("3.1.1", false)]
        [InlineData("3.2.0", false)]
        [InlineData("3.3.0", true)]
        [InlineData("3.4.0", true)]
        [InlineData("3.5.1", true)]
        [InlineData("4.0.0", true)]
        [InlineData("4.0.4", true)]
        public void RetainedVersionsConsumeTheExpectedTaskContextLayout(string version, bool hasCpus)
        {
            const int Sentinel = 123456789;
            using var stream = new MemoryStream();
            // Write the wire fields directly, independently of PayloadWriter.
            SerDe.Write(stream, true);
            SerDe.Write(stream, 42);
            SerDe.Write(stream, "secret");
            SerDe.Write(stream, 17);
            SerDe.Write(stream, 3);
            SerDe.Write(stream, 2);
            SerDe.Write(stream, 4294967301L);
            if (hasCpus)
            {
                SerDe.Write(stream, 4);
            }

            SerDe.Write(stream, 1); // Resource count.
            SerDe.Write(stream, "gpu");
            SerDe.Write(stream, "gpu");
            SerDe.Write(stream, 2); // Resource addresses.
            SerDe.Write(stream, "0");
            SerDe.Write(stream, "1");
            SerDe.Write(stream, 1); // Local property count.
            SerDe.Write(stream, "spark.job.description");
            SerDe.Write(stream, "retained framing");
            SerDe.Write(stream, Sentinel);
            stream.Position = 0;

            TaskContext context = new TaskContextProcessor(new Version(version)).Process(stream);

            Assert.True(context.IsBarrier);
            Assert.Equal(42, context.Port);
            Assert.Equal("secret", context.Secret);
            Assert.Equal(17, context.StageId);
            Assert.Equal(3, context.PartitionId);
            Assert.Equal(2, context.AttemptNumber);
            Assert.Equal(4294967301L, context.AttemptId);
            Assert.Equal(hasCpus ? 4 : 0, context.CPUs);
            Assert.Empty(context.Resources);
            Assert.Single(context.LocalProperties);
            Assert.Equal("retained framing", context.LocalProperties["spark.job.description"]);
            Assert.Equal(Sentinel, SerDe.ReadInt32(stream));
            Assert.Equal(stream.Length, stream.Position);
        }
    }
}
