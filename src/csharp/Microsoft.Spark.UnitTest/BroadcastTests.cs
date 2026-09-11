// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Reflection;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;
using Moq;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class BroadcastTests
    {
        [Theory]
        [InlineData("3.0.0", true)]
        [InlineData("3.1.1", true)]
        [InlineData("3.2.0", true)]
        [InlineData("3.3.0", true)]
        [InlineData("3.4.0", true)]
        [InlineData("3.5.3", true)]
        [InlineData("4.0.0", true)]
        [InlineData("4.0.1", true)]
        [InlineData("4.0.2", true)]
        [InlineData("4.0.3", true)]
        [InlineData("4.0.4", true)]
        [InlineData("2.0.0", false)]
        [InlineData("2.3.0", false)]
        [InlineData("2.4.0", false)]
        [InlineData("2.4.8", false)]
        [InlineData("4.1.0", false)]
        [InlineData("5.0.0", false)]
        public void CreationDispatchesOnlySupportedVersions(string version, bool supported)
        {
            var jvm = new Mock<IJvmBridge>();
            var contextRef = new JvmObjectReference("context", jvm.Object);
            var confRef = new JvmObjectReference("conf", jvm.Object);
            var javaContextRef = new JvmObjectReference("javaContext", jvm.Object);
            jvm.Setup(m => m.CallNonStaticJavaMethod(contextRef, "getConf")).Returns(confRef);
            jvm.Setup(m => m.CallNonStaticJavaMethod(
                confRef, "get", "spark.io.encryption.enabled", "false")).Returns("false");
            var context = new SparkContext(contextRef);
            jvm.Setup(m => m.CallStaticJavaMethod(
                "org.apache.spark.api.java.JavaSparkContext", "fromSparkContext", context))
                .Returns(javaContextRef);
            var reachedAdapter = new InvalidOperationException("Reached existing broadcast adapter.");
            jvm.Setup(m => m.CallStaticJavaMethod(
                "org.apache.spark.api.python.PythonRDD", "setupBroadcast", It.IsAny<object>()))
                .Throws(reachedAdapter);

            Exception exception = Record.Exception(() =>
                new Broadcast<int>().CreateBroadcast(context, 42, new Version(version)));
            if (supported)
            {
                Assert.Same(reachedAdapter, exception);
            }
            else
            {
                Assert.IsType<NotSupportedException>(exception);
                jvm.Verify(m => m.CallStaticJavaMethod(
                    "org.apache.spark.api.python.PythonRDD", "setupBroadcast", It.IsAny<object>()),
                    Times.Never);
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(10)]
        [InlineData(100000)]
        public void EncryptionUploadUsesInt32ChunkLength(int valueLength)
        {
            string value = new string('x', valueLength);
            using var expected = new MemoryStream();
            BinarySerDe.Serialize(expected, value);
            using var upload = new MemoryStream();

            // Exercise the upload writer without a JVM or encryption server.
            typeof(Broadcast<string>).GetMethod("WriteToStream",
                BindingFlags.NonPublic | BindingFlags.Instance)
                .Invoke(new Broadcast<string>(), new object[] { value, upload });

            upload.Position = 0;
            Assert.Equal(expected.Length, SerDe.ReadInt32(upload));
            Assert.Equal(expected.ToArray(), SerDe.ReadBytes(upload, (int)expected.Length));
            Assert.Equal(-1, SerDe.ReadInt32(upload));
            Assert.Equal(upload.Length, upload.Position);
        }
    }
}
