// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Reflection;
using Microsoft.Spark.Interop.Ipc;
using Xunit.Sdk;

namespace Microsoft.Spark.E2ETest
{
    /// <summary>
    /// Records method boundaries without waiting for the test runner to flush its output.
    /// </summary>
    public sealed class E2ETraceAttribute : BeforeAfterTestAttribute
    {
        public override void Before(MethodInfo methodUnderTest)
        {
            E2EHangDiagnostics.BeginTest();
            IpcDebugTrace.Write(
                $"test-begin {methodUnderTest.DeclaringType.FullName}.{methodUnderTest.Name}");
        }

        // This is a method-return marker, not a pass verdict; the test runner owns results.
        public override void After(MethodInfo methodUnderTest)
        {
            IpcDebugTrace.Write(
                $"test-end {methodUnderTest.DeclaringType.FullName}.{methodUnderTest.Name}");
            E2EHangDiagnostics.EndTest();
        }
    }
}
