// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Reflection;
using System.Text;
using Microsoft.Spark.Interop.Ipc;
using Xunit;

namespace Microsoft.Spark.Worker.UnitTest
{
    [CollectionDefinition("Arrow logging failures", DisableParallelization = true)]
    public sealed class ArrowLoggingFailureCollection
    {
    }

    [Collection("Arrow logging failures")]
    public class ArrowLoggingFailureTests
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void DiagnosticFailureDoesNotReplaceTaskFailure(bool failDuringReport)
        {
            var primary = new InvalidOperationException("original-task-read-failure");
            using var input = new FailingInput(primary);
            using var output = new ReportingOutput(failDuringReport);
            var runner = new TaskRunner(1231, null, false, new Version(3, 3, 4));
            MethodInfo process = typeof(TaskRunner).GetMethod(
                "ProcessStream", BindingFlags.Instance | BindingFlags.NonPublic);
            TextWriter original = Console.Out;
            var brokenLogger = new FailingLogWriter(original, failDuringReport);
            TargetInvocationException observed;
            try
            {
                Console.SetOut(brokenLogger);
                observed = Assert.Throws<TargetInvocationException>(() => process.Invoke(
                    runner, new object[] { input, output, new Version(3, 3, 4), false }));
            }
            finally
            {
                Console.SetOut(original);
            }

            Assert.Equal(1, brokenLogger.Failures);
            Assert.Same(primary, observed.InnerException);
            if (failDuringReport)
            {
                Assert.Equal(2, output.Length);
                Assert.Equal(1, output.WriteCalls);
                Assert.Equal(0, output.FlushCalls);
            }
            else
            {
                output.Position = 0;
                Assert.Equal((int)SpecialLengths.PYTHON_EXCEPTION_THROWN, SerDe.ReadInt32(output));
                Assert.Contains(primary.Message, SerDe.ReadString(output));
                Assert.Equal(output.Length, output.Position);
                Assert.Equal(1, output.FlushCalls);
            }
        }

        private sealed class FailingInput : MemoryStream
        {
            private readonly Exception _failure;

            internal FailingInput(Exception failure) => _failure = failure;

            public override int Read(byte[] buffer, int offset, int count) => throw _failure;
        }

        private sealed class ReportingOutput : MemoryStream
        {
            private readonly bool _fail;

            internal ReportingOutput(bool fail) => _fail = fail;

            internal int WriteCalls { get; private set; }

            internal int FlushCalls { get; private set; }

            public override void Write(byte[] buffer, int offset, int count)
            {
                ++WriteCalls;
                if (_fail)
                {
                    base.Write(buffer, offset, Math.Min(2, count));
                    throw new IOException("exception-report-write-failure");
                }

                base.Write(buffer, offset, count);
            }

            public override void Flush() => ++FlushCalls;
        }

        private sealed class FailingLogWriter : TextWriter
        {
            private readonly TextWriter _original;
            private readonly string _failingMessage;

            internal FailingLogWriter(TextWriter original, bool failDuringReport)
            {
                _original = original;
                _failingMessage = failDuringReport ?
                    "Writing exception to stream" : "ProcessStream() failed";
            }

            public override Encoding Encoding => Encoding.UTF8;

            internal int Failures { get; private set; }

            public override void WriteLine(string value)
            {
                if (value != null && value.Contains("[1231]") && value.Contains(_failingMessage))
                {
                    ++Failures;
                    throw new IOException("logger-output-failure");
                }

                _original.WriteLine(value);
            }
        }
    }
}
