// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Microsoft.Spark.Worker.Processor;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class BroadcastVariableProcessorTests
    {
        private const int Sentinel = 123456789;
        private static long s_nextId = 800000;

        [Theory]
        [InlineData("3.0.0")]
        [InlineData("3.5.3")]
        [InlineData("4.0.4")]
        public void PlaintextBroadcastsSupportRemoveRetainAndReAdd(string version)
        {
            using var directory = new TemporaryDirectory();
            long firstId = Interlocked.Increment(ref s_nextId);
            long secondId = Interlocked.Increment(ref s_nextId);
            string firstPath = Path.Combine(directory.Path, "first");
            string secondPath = Path.Combine(directory.Path, "second");
            WriteFile(firstPath, "first value");
            WriteFile(secondPath, new[] { 10, 20 });
            var processor = new BroadcastVariableProcessor(new Version(version));
            try
            {
                using (MemoryStream frame = CreatePlaintextFrame(
                    (firstId, firstPath), (secondId, secondPath)))
                {
                    Assert.Equal(2, processor.Process(frame).Count);
                    Assert.Equal(Sentinel, SerDe.ReadInt32(frame));
                    Assert.Equal(frame.Length, frame.Position);
                }

                Assert.Equal("first value", BroadcastRegistry.Get(firstId));
                Assert.Equal(new[] { 10, 20 }, Assert.IsType<int[]>(BroadcastRegistry.Get(secondId)));
                using (MemoryStream frame = CreatePlaintextFrame())
                {
                    Assert.Equal(0, processor.Process(frame).Count);
                    Assert.Equal(Sentinel, SerDe.ReadInt32(frame));
                }

                using (MemoryStream frame = CreatePlaintextFrame((-firstId - 1, null)))
                {
                    processor.Process(frame);
                    Assert.Equal(Sentinel, SerDe.ReadInt32(frame));
                }

                Assert.Throws<KeyNotFoundException>(() => BroadcastRegistry.Get(firstId));
                Assert.Equal(new[] { 10, 20 }, Assert.IsType<int[]>(BroadcastRegistry.Get(secondId)));
                WriteFile(firstPath, "replacement");
                using (MemoryStream frame = CreatePlaintextFrame((firstId, firstPath)))
                {
                    processor.Process(frame);
                    Assert.Equal(Sentinel, SerDe.ReadInt32(frame));
                }

                Assert.Equal("replacement", BroadcastRegistry.Get(firstId));
            }
            finally
            {
                BroadcastRegistry.Remove(firstId);
                BroadcastRegistry.Remove(secondId);
            }
        }

        [Theory]
        [InlineData("3.0.0")]
        [InlineData("3.5.3")]
        [InlineData("4.0.4")]
        public async Task EncryptedBroadcastsCompleteBeforeServerEof(string version)
        {
            long firstId = Interlocked.Increment(ref s_nextId);
            long secondId = Interlocked.Increment(ref s_nextId);
            string largeValue = new string('x', 200000);
            byte[] bytes = CreateEncryptedValues((firstId, "first"), (secondId, largeValue));
            try
            {
                var result = await RunEncryptedAsync(
                    version, bytes, new[] { firstId, secondId });

                Assert.Null(result.Error);
                Assert.Equal((int)'1', result.CompletionByte);
                Assert.True(result.Closed);
                Assert.Equal("first", BroadcastRegistry.Get(firstId));
                Assert.Equal(largeValue, BroadcastRegistry.Get(secondId));
            }
            finally
            {
                BroadcastRegistry.Remove(firstId);
                BroadcastRegistry.Remove(secondId);
            }
        }

        [Theory]
        [InlineData("mismatch")]
        [InlineData("truncated")]
        [InlineData("malformed")]
        [InlineData("authentication")]
        [InlineData("deserialization")]
        public async Task EncryptedFailureClosesSocketWithoutSuccessAck(string failure)
        {
            long id = Interlocked.Increment(ref s_nextId);
            byte[] bytes = CreateEncryptedValues((failure == "mismatch" ? id + 1 : id, "value"));
            if (failure == "truncated")
            {
                Array.Resize(ref bytes, bytes.Length - 1);
            }
            else if (failure == "malformed")
            {
                // A raw big-endian ID followed by MessagePack's reserved invalid code.
                Array.Resize(ref bytes, sizeof(long) + 1);
                bytes[sizeof(long)] = 0xc1;
            }
            else if (failure == "deserialization")
            {
                bytes = CreateEncryptedValues((id, new UnmarkedValue { Value = "disallowed" }));
            }

            try
            {
                var result = await RunEncryptedAsync(
                    "4.0.4", bytes, new[] { id }, failure);

                Assert.NotNull(result.Error);
                Assert.Equal(-1, result.CompletionByte);
                Assert.True(result.Closed);
                Assert.Throws<KeyNotFoundException>(() => BroadcastRegistry.Get(id));
            }
            finally
            {
                BroadcastRegistry.Remove(id);
            }
        }

        [Fact]
        public void MissingPlaintextFileFailsBeforeTheFollowingCommand()
        {
            using var directory = new TemporaryDirectory();
            long id = Interlocked.Increment(ref s_nextId);
            using MemoryStream frame = CreatePlaintextFrame(
                (id, Path.Combine(directory.Path, "missing")));

            Assert.Throws<FileNotFoundException>(() =>
                new BroadcastVariableProcessor(new Version("4.0.4")).Process(frame));
            Assert.Equal(Sentinel, SerDe.ReadInt32(frame));
            Assert.Throws<KeyNotFoundException>(() => BroadcastRegistry.Get(id));
        }

        private static async Task<(Exception Error, int CompletionByte, bool Closed)>
            RunEncryptedAsync(string version, byte[] bytes, long[] ids, string failure = null)
        {
            const string secret = "broadcast-unit-secret";
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            Task<(int CompletionByte, bool Closed)> server = Task.Run(async () =>
            {
                using TcpClient client = await listener.AcceptTcpClientAsync()
                    .WaitAsync(TimeSpan.FromSeconds(10));
                client.ReceiveTimeout = 3000;
                client.SendTimeout = 3000;
                using NetworkStream stream = client.GetStream();
                Assert.Equal(secret, SerDe.ReadString(stream));
                // Spark's SocketAuthHelper uses a length-prefixed UTF-8 "ok" reply.
                SerDe.Write(stream, failure == "authentication" ? "error" : "ok");
                stream.Flush();
                if (failure != "authentication")
                {
                    stream.Write(bytes, 0, bytes.Length);
                    stream.Flush();
                }

                if (failure == "truncated")
                {
                    client.Client.Shutdown(SocketShutdown.Send);
                }

                try
                {
                    int completion = stream.ReadByte();
                    return (completion, completion == -1 || stream.ReadByte() == -1);
                }
                catch (IOException)
                {
                    // Bound regressions that wait for server EOF or leak the client socket.
                    return (-2, false);
                }
            });

            using var frame = new MemoryStream();
            // Spark v4.0.4 PythonWorkerUtils.writeBroadcasts (also v3.5.3 PythonRunner).
            SerDe.Write(frame, true);
            SerDe.Write(frame, ids.Length);
            SerDe.Write(frame, port);
            SerDe.Write(frame, secret);
            foreach (long id in ids)
            {
                SerDe.Write(frame, id);
            }

            SerDe.Write(frame, Sentinel);
            frame.Position = 0;
            Exception error = Record.Exception(() =>
                new BroadcastVariableProcessor(new Version(version)).Process(frame));
            if (error == null)
            {
                Assert.Equal(Sentinel, SerDe.ReadInt32(frame));
                Assert.Equal(frame.Length, frame.Position);
            }

            var observed = await server.WaitAsync(TimeSpan.FromSeconds(10));
            return (error, observed.CompletionByte, observed.Closed);
        }

        private static byte[] CreateEncryptedValues(params (long Id, object Value)[] entries)
        {
            using var stream = new MemoryStream();
            foreach (var entry in entries)
            {
                SerDe.Write(stream, entry.Id);
                BinarySerDe.Serialize(stream, entry.Value);
            }

            return stream.ToArray();
        }

        private static MemoryStream CreatePlaintextFrame(params (long Id, string Path)[] entries)
        {
            var stream = new MemoryStream();
            SerDe.Write(stream, false);
            SerDe.Write(stream, entries.Length);
            foreach (var entry in entries)
            {
                SerDe.Write(stream, entry.Id);
                if (entry.Id >= 0)
                {
                    SerDe.Write(stream, entry.Path);
                }
            }

            SerDe.Write(stream, Sentinel);
            stream.Position = 0;
            return stream;
        }

        private static void WriteFile(string path, object value)
        {
            using FileStream stream = File.Create(path);
            BinarySerDe.Serialize(stream, value);
        }

        public sealed class UnmarkedValue
        {
            public string Value { get; set; }
        }
    }
}
