// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
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
    }
}
