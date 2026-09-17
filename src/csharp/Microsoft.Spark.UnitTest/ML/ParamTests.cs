// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Feature.Param;
using Moq;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    [Collection("Spark Unit Tests")]
    public class ParamTests
    {
        private const string ParamClassName = "org.apache.spark.ml.param.Param";

        [Fact]
        public void Spark400PassesClassTagToConstructor()
        {
            WithJvmBridge(jvm =>
            {
                var classTag = new JvmObjectReference("classTag", null);
                var expected = new JvmObjectReference("param", null);
                jvm.Setup(bridge => bridge.CallStaticJavaMethod(
                    "scala.reflect.ClassTag", "Any", It.Is<object[]>(args => args.Length == 0)))
                    .Returns(classTag);
                jvm.Setup(bridge => bridge.CallConstructor(
                    ParamClassName,
                    It.Is<object[]>(args => args.Length == 4 &&
                        Equals(args[0], "parent") && Equals(args[1], "name") &&
                        Equals(args[2], "doc") && ReferenceEquals(args[3], classTag))))
                    .Returns(expected);

                Assert.Same(expected, Param.CreateJvmParam(
                    "parent", "name", "doc", new Version("4.0.0")));
                jvm.VerifyAll();
                Assert.Equal(2, jvm.Invocations.Count);
            });
        }

        [Theory]
        [InlineData("3.0.0")]
        [InlineData("3.1.1")]
        [InlineData("3.2.0")]
        [InlineData("3.3.0")]
        [InlineData("3.4.0")]
        [InlineData("3.5.3")]
        [InlineData("4.0.1")]
        [InlineData("4.0.2")]
        [InlineData("4.0.3")]
        [InlineData("4.0.4")]
        public void OtherSupportedVersionsRetainThreeArgumentConstructor(string version)
        {
            WithJvmBridge(jvm =>
            {
                var expected = new JvmObjectReference("param", null);
                jvm.Setup(bridge => bridge.CallConstructor(
                    ParamClassName,
                    It.Is<object[]>(args => args.Length == 3 &&
                        Equals(args[0], "parent") && Equals(args[1], "name") &&
                        Equals(args[2], "doc"))))
                    .Returns(expected);

                Assert.Same(expected, Param.CreateJvmParam(
                    "parent", "name", "doc", new Version(version)));
                jvm.VerifyAll();
                Assert.Single(jvm.Invocations);
            });
        }

        [Fact]
        public void ClassTagFailurePropagatesWithoutConstructingParam()
        {
            WithJvmBridge(jvm =>
            {
                var expected = new InvalidOperationException("ClassTag lookup failed.");
                jvm.Setup(bridge => bridge.CallStaticJavaMethod(
                    "scala.reflect.ClassTag", "Any", It.Is<object[]>(args => args.Length == 0)))
                    .Throws(expected);

                Assert.Same(expected, Assert.Throws<InvalidOperationException>(() =>
                    Param.CreateJvmParam("parent", "name", "doc", new Version("4.0.0"))));
                jvm.VerifyAll();
                Assert.Single(jvm.Invocations);
            });
        }

        [Theory]
        [InlineData("3.5.3", false)]
        [InlineData("4.0.0", true)]
        [InlineData("4.0.1", false)]
        public void ConstructorFailurePropagatesWithoutRetry(string version, bool usesClassTag)
        {
            WithJvmBridge(jvm =>
            {
                var expected = new InvalidOperationException("Param construction failed.");
                if (usesClassTag)
                {
                    jvm.Setup(bridge => bridge.CallStaticJavaMethod(
                        "scala.reflect.ClassTag", "Any", It.Is<object[]>(args => args.Length == 0)))
                        .Returns(new JvmObjectReference("classTag", null));
                }

                jvm.Setup(bridge => bridge.CallConstructor(ParamClassName, It.IsAny<object[]>()))
                    .Throws(expected);

                Assert.Same(expected, Assert.Throws<InvalidOperationException>(() =>
                    Param.CreateJvmParam("parent", "name", "doc", new Version(version))));
                jvm.VerifyAll();
                Assert.Equal(usesClassTag ? 2 : 1, jvm.Invocations.Count);
            });
        }

        private static void WithJvmBridge(Action<Mock<IJvmBridge>> test)
        {
            IJvmBridge previousBridge = SparkEnvironment.JvmBridge;
            var jvm = new Mock<IJvmBridge>(MockBehavior.Strict);
            SparkEnvironment.JvmBridge = jvm.Object;
            try
            {
                test(jvm);
            }
            finally
            {
                SparkEnvironment.JvmBridge = previousBridge;
            }
        }
    }
}
