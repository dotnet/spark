// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using Xunit.Abstractions;

namespace Microsoft.Spark.UnitTest.TestUtils
{
    // Tests can subclass this to get Console output to display when using
    // xUnit testing framework.
    // Workaround found at https://github.com/microsoft/vstest/issues/799
    public class XunitConsoleOutHelper : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly TextWriter _originalOut;
        private readonly TextWriter _textWriter;

        public XunitConsoleOutHelper(ITestOutputHelper output)
        {
            _output = output;
            _originalOut = Console.Out;
            _textWriter = new StringWriter();
            Console.SetOut(_textWriter);
        }

        public void Dispose()
        {
            _output.WriteLine(_textWriter.ToString());
            Console.SetOut(_originalOut);
        }
    }
}
